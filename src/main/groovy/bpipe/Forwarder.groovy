/*
* Copyright (c) 2012 MCRI, authors
*
* Permission is hereby granted, free of charge, to any person
* obtaining a copy of this software and associated documentation
* files (the "Software"), to deal in the Software without
* restriction, including without limitation the rights to use,
* copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the
* Software is furnished to do so, subject to the following
* conditions:
*
* The above copyright notice and this permission notice shall be
* included in all copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
* EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
* OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
* NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
* HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
* WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
* THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
package bpipe

import groovy.util.logging.Log

/**
 * Continuously monitors files and forwards (or 'tails') their outputs to 
 * specific destinations.  The main purpose of this class is to avoid
 * having all the files be continuously open which is necessary when there
 * are limits on the number of open files.
 * 
 * @author simon.sadedin@mcri.edu.au
 */
@Log
class Forwarder extends TimerTask {
    
    /**
     * Global list of all forwarders
     */
    static List<Forwarder> forwarders = []
    
    /**
     * Timer that schedules polling of files that are forwarded from Torque jobs
     */
    static Timer forwardingTimer
    
    /**
     * Longest amount of time we will wait for an expected file that does not exist
     * to appear
     */
    static long MAX_FLUSH_WAIT = 10000
    //bsub job id
    private String bsubJobid
    private String CMD_EXIT_FILE 
    //define error state here if we see EXIT state of job  once it should not be reinitialised to 0
    private LsfExit = [:]
    /**
     * The list of files that are being 'tailed'
     */
    List<File> files = []
    
    /**
     * Current position in each file that we know about
     */
    Map<File, Long> filePositions = [:]
    
    /**
     * Destinations to which the file should be forwarded
     */
    Map<File, OutputStream> fileDestinations = [:]
    
   // Forwarder(File f, OutputStream out) {
   //    forward(f,out)
   //  }
   
    Forwarder(File f, OutputStream out, String jobid) {
	forward(f,out,jobid)
    }
    void forward(File file, OutputStream out, String jobid) {
	bsubJobid = jobid
        synchronized(files) {
            files << file
	    // get file name without extension
	    CMD_EXIT_FILE=file.absolutePath.lastIndexOf('.').with {it != -1 ? file.absolutePath[0..<it] : file.absolutePath}
	    //
            fileDestinations[file] = out
            filePositions[file] = file.exists()? file.length() : 0L
        }
    }
    
    void cancel(File file) {
        synchronized(files) {
            files.remove(file)
            fileDestinations.remove(file)
            filePositions.remove(file)
        }
    }

    /**
     * Attempt to wait until all the expected files exist, then run forwarding
     */
    public void flush() {
        synchronized(files) {
            long startTimeMs = System.currentTimeMillis()
            long now = startTimeMs
            
            this.files.collect { it.parentFile }.unique { it.canonicalFile.absolutePath }*.listFiles()
            
            while(now - startTimeMs < MAX_FLUSH_WAIT) {
                if(this.files.every { it.exists() })
                    break
                now = System.currentTimeMillis()
                Thread.sleep(1000)
            }
            if(now - startTimeMs >= MAX_FLUSH_WAIT) {
                def msg = "Exceeded $MAX_FLUSH_WAIT ms waiting for one or more output files ${files*.absolutePath} to appear: output may be incomplete"
                System.err.println  msg
                log.warning msg
            }
            else {
                log.info "All files ${files*.absolutePath} exist"
            }
        }
        this.run()
    }
    @Override
    public void run() {
        List<File> scanFiles
        synchronized(files) {
            try {
                
		String FileName="${CMD_EXIT_FILE}.exit"
		// sb43 This file stores status for LSF exited jobs if any
		String FileNameExit="${CMD_EXIT_FILE}.lsf"
		// sb43 method to check bsub status
		if(bsubJobid && !(new File(FileName).exists() )) {
			int state_exit = 0
			int state_run = 0
			int state_done = 0
			(state_exit, state_run, state_done)=parseBjobs(bsubJobid,FileName,FileNameExit)
			log.info "Current bsub job[$bsubJobid] status: EXIT:$state_exit RUN:$state_run DONE:$state_done"
		}
// check if .exit file exists which is indication of job completion
		if(new File(FileName).exists()){
			return
		}
		// method call completed 
		scanFiles = files.clone().grep { it.exists() }
                byte [] buffer = new byte[8096]
                log.info "Scanning ${scanFiles.size()} / ${files.size()} files "
                for(File f in scanFiles) {
                    try {
                        f.withInputStream { ifs ->
                            long skip = filePositions[f]
			    ifs.skip(skip)
                            int count = ifs.read(buffer)
                            if(count < 0) {
                                log.info "No chars to read from ${f.absolutePath} (size=${f.length()}) "
                                return
                            }
                            
                            log.info "Read " + count + " chars from $f starting with " + Utils.truncnl(new String(buffer, 0, Math.min(count,30)),25)
                            
                            // TODO: for neater output we could trim the output to the 
                            // most recent newline here
                            fileDestinations[f].write(buffer,0,count)
                            filePositions[f] = filePositions[f] + count
                        }
                    }
                    catch(Exception e) {
                        //log.warning "Unable to read file $f  $bsubJobid"
                        log.warning "Unable to read file $f"
                        e.printStackTrace()
                    }
                }
            }
            catch(Exception e) {
                log.severe("Failure in output forwarding")
                e.printStackTrace()
            }
        }
    }

/* 
* sb43 method to parse bjobs output
* Takes bjobs output stream a input and parse parse each line to get value of STAT column
* if stat colum shows EXIT then bpipe will terminate the process only if there is no other 
* jobs in same array with STAT other than EXIT or DONE present 
* -w to get bjobs output in one line
*/
  def parseBjobs(bsubJobid,FileName,FileNameExit) {
	
	int counter = 0
	int state_e = 0 // EXIT
	int state_r = 0 // RUNNING
	int state_d = 0 // DONE 
	
	def bjobsCmd="bjobs -w "+bsubJobid
	ProcessBuilder bj = new ProcessBuilder("bash", "-c", bjobsCmd)
	Process b = bj.start()
	Utils.withStreams(b) {
		StringBuilder out = new StringBuilder()
		StringBuilder err = new StringBuilder()
		b.waitForProcessOutput(out, err)
		int exitValue = b.waitFor()
		// non zero exit value: bsub  not executed
		if(exitValue != 0) {
			reportStartError(bjobsCmd, out,err,exitValue)
			throw new PipelineError("Failed to start command:\n\n$bjobsCmd")
		}
		if(err && exitValue == 0) {
			
			log.info "bsub job : $bsubJobid job not yet started"
			
		}
		// parse bjobs output
		if(out && exitValue == 0) {
		    def job_array=out.toString().split('\n').collect{it as String}
		    for ( line in job_array) {
			    counter++
			    def job_line = line.split().collect{it as String}
			    if(!(new File(FileNameExit).exists()) && job_line[2] == "EXIT" && counter > 1 ) {
			    		new File(FileNameExit).write("1")			
			      	LsfExit[(line)] = 1
			      	state_e = 1
			    }
			    if(job_line[2] != null && job_line[2] != "EXIT" && job_line[2] != "DONE" && counter >1) {
			      state_r = 1
			      //one job evidence sufficient to keep bpipe running
			      break
			    }
			    if(job_line[2] == "DONE" && counter >1) {
			      state_d = 1
			    }
		    }
		    // Create file with exit status non zero if we find one of the array job has non exit status		
		    if( (new File(FileNameExit).exists()) && !state_r) {
		      log.info "Exit status written in $FileName"
		      new File(FileName).write("1")
		      // print jobs with EXIT status 
		      LsfExit.each {
		          	println it.key
			  }
		    }
		    // Create file with exit status zero if we did not see job status EXIT and all the jobs are completed		
		    if(!(new File(FileNameExit).exists()) && !state_r && state_d) {
		      log.info "Exit status written in $FileName"
		      new File(FileName).write("0")
		    }
		}
	}
	return [state_e, state_r, state_d]
    }
// end of bjobs methods

}
