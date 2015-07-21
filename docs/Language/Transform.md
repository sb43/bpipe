# The Transform Statement

    
    transform(<transform name>) {
        < statements to transform inputs >
    
    }

    
    transform(<input file pattern>) to(replacement pattern)  {
        < statements to transform inputs >
    
    }
### Behavior

Transform is a convenient alias for [produce](Language/Produce) where the name of the
output or outputs is deduced from the name of the input(s) by modifying the
file extension.   For example, if you have a command that converts 
a CSV file called *foo*.csv to an XML file, you can easily declare a section of your script
to output *foo*.xml using a transform with the name 'xml'.

The output(s) that are automatically deduced by *transform* will inherit all the behavior implied by the [produce](Language/Produce) statement.

Since version 0.9.8.4, transform has offered an extended form that allows you to do more than just replace the file extension. This form uses two parts, taking the form: 
  
  `transform(<input file pattern>) to(<output file pattern>`) { ... }

The input and output patterns are assumed to match to the end of the file name, but can include a regular expression pattern for matching the input files.

*Note*: input file patterns that contain no regular expression characters, or that end in "." followed by plain characters are treated as file extensions. ie: ".xml" is treated as ".xml", not "any character followed by xml".

### Annotation

You can also declare a whole pipeline stage as a transform by adding the Transform annotation prior to the stage in the form `@Transform(<filter name>)`. This form is a bit less flexible, but more concise when you don't need the flexibility.

### Examples

**Remove Comment Lines from CSV File**
```groovy 

transform("xml") {
  exec """
    csv2xml $input > $output
  """
}
```

**Run FastQC on a Gzipped FASTQ file**
Fastqc produces output files following an unusual convention for naming. To match this convention, we can use the extended form of transform:
```groovy 

fastqc = {
    transform('.fastq.gz') to('_fastqc.zip') {
        exec "fastqc -o . --noextract $inputs"
    }
    forward input
}
```

Note also that since the output zip files from FastQC are usually not used downstream, we forward the input files rather than the default of letting the output files be forwarded to the next stage.
