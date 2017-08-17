package me.jairam

import java.io.File

import me.jairam.avro.AvroWriter
import me.jairam.csv.CSVReader
import org.rogach.scallop.ScallopConf

class CsvAvroConverter(args: Seq[String]) extends ScallopConf(args) {
  val in = opt[File](required = true, descr = "Input CSV")
  val out = opt[String](required = true, descr = "Output file name")

  verify()
}

object CsvAvroConverter extends App {

  val cli = new CsvAvroConverter(args)
  val input = cli.in()
  val output = new File(cli.out())

  val csvReader = new CSVReader(input)
  val avroWriter = new AvroWriter(output)

  for {
    schema <- csvReader.inferSchema()
  } {
    val avroSchema = avroWriter.buildSchema(schema, input.getName, input.getParent)
    avroWriter.write(csvReader.rows(), avroSchema)
  }
  
  println(s"${output.getAbsolutePath} created")
}
