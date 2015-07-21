/*
 * Copyright (c) 2013 MCRI, authors
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

import groovy.util.logging.Log;
import bpipe.executor.CommandExecutor;

@Log
class Command implements Serializable {
    
    public static final long serialVersionUID = 0L
    
    /**
     * Bpipe id for the command - can be used by a {@link CommandExecutor} to
     * create unique files as it is guaranteed to be unique to this command
     */
    String id
    
    /**
     * Human readable short name for the command. Usually this is set to just
     * the stage name of the pipeline executing the command (so not guaranteed to
     * be unique!)
     */
    String name

    /**
     * This command's Branch
     */
    Branch branch

    /**
     * Actual command to be executed.
     */
    String command
    
    List outputs = []
    
    CommandExecutor executor
    
    String configName
    
    /**
     * When the command was initiated (eg: queued in a queuing system)
     */
    long createTimeMs
    
    /**
     * When the command started running
     */
    long startTimeMs = -1
    
    /**
     * When the command finished running (if we know)
     */
    long stopTimeMs
    
    /**
     * The last observed status for the command. This is 
     * set optionally by the executor
     */
    CommandStatus status
    
    /**
     * The exit code of the command IF it has exited
     */
    int exitCode = -1
    
    /**
     * Internal configuration = accessed via getConfig()
     */
    private Map cfg
    
    Map getConfig(inputs) {
        if(cfg != null)
            return cfg
            
        // How to run the job?  look in user config
        if(!configName)
            configName = command.trim().split(/\s/)[0].trim()
        
        log.info "Checking for configuration for command $configName"
        
        // Use default properties from root entries into user config
        def defaultConfig = Config.userConfig.findAll { !(it.value instanceof Map) }
        log.info "Default command properties: $defaultConfig"
        
        def rawCfg = defaultConfig
        
        def cmdConfig = Config.userConfig.containsKey("commands")?Config.userConfig.commands[configName]:null
        if(cmdConfig && cmdConfig instanceof Map)  {
            // override properties in default config with those for the
            // specific command
            rawCfg = defaultConfig + cmdConfig
        }
        
        // Make a new map
        this.cfg = rawCfg.clone()
        
        // Resolve inputs to files
        List fileInputs = inputs.collect { new File(it) }
        
        // Execute any executable properties that are closures
        rawCfg.each { key, value ->
            if(value instanceof Closure) {
                cfg[key] = value(fileInputs)
            }
            
            // Special case - walltime can be specified as integer number of seconds
            if(key == 'walltime' && !(cfg[key] instanceof String)) {
                cfg[key] = formatWalltime(cfg[key])
                log.info "Converted walltime is " + cfg[key]
            }
        }
        return cfg
    }
    
    private String formatWalltime(def walltime) {
       // Treat as integer, convert to string
       walltime = walltime.toInteger()
       int hours = (int)Math.floor(walltime / 3600)
       int minutes = (int)Math.floor((walltime - hours*3600)/60)
       int seconds = walltime % 60
       return String.format('%02d:%02d:%02d', hours, minutes, seconds )
    }
    
    public void setStatus(String statusValue) {
        
        if(statusValue != this.status?.name())
            log.info "Command $id changing state from ${this.status} to $statusValue"
            
        try {
            CommandStatus statusEnum = CommandStatus.valueOf(statusValue)
            if(statusEnum != this.status) {
                if(statusEnum == CommandStatus.RUNNING)
                    this.startTimeMs = System.currentTimeMillis()
            }
            this.status = statusEnum
                
        }
        catch(Exception e) {
            log.warning("Failed to update status for result $statusValue: $e")
        }
    }
}
