# CSV to AVRO Converter

## Introduction

A simple library and CLI tool to converter a CSV file to Avro file. 
The main challenge when doing this is generating the Avro Schema. To that 
end, this tool borrows very heavily from 
[Spark's CSV Inference code](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/csv/CSVInferSchema.scala).


## Prerequisites 

- [JDK][jdk] v1.8+

## Modules

There are two modules in this tool:

- cli
- lib


### cli

Command line tool to convert a CSV to Avro

```sh
csv-avro-conveter -i input.csv -o output.avro
```

### lib

This can be used in any project where the conversion might be required. 

```scala
  import me.jairam.csv.CsvReader
  import me.jairam.avro.AvroWriter
  import me.jairam.schema.Builder.buildSchema
  
  val csvReader = new CsvReader(inputFile)
  val avroWriter = new AvroWriter(outputFile)

  for {
    rows <- csvReader.rows()
    internalSchema <- csvReader.inferSchema()
    avroSchema <- buildSchema(internalSchema, input.getName)
  } {
    avroWriter.write(rows, avroSchema)
  }
```


[jdk]: http://www.oracle.com/technetwork/java/javase/downloads/index.html