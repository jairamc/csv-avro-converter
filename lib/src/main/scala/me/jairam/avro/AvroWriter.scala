package me.jairam.avro

import java.io.File

import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import me.jairam.schema._
import org.apache.avro.Schema

class AvroWriter(file: File) {

  def write(rows: Iterator[Array[String]], schema: Schema): Unit = {
    val datumWriter = new GenericDatumWriter[GenericRecord](schema)
    val writer = new DataFileWriter[GenericRecord](datumWriter)
    writer.create(schema, file)

    try {
      val headers = rows.next()

      for (row <- rows) {
        val record = new Record(schema)
        for (i <- headers.indices) {
          record.put(headers(i), schema.getField(headers(i)).cast(row(i)))
        }
        writer.append(record)
      }
    } catch {
      case e: Exception =>
        // If there is an exception while creating Avro files, clean up and delete the avro file created
        if (file.exists()) file.delete()
        throw e
    } finally {
      writer
        .close() // Closing the writer even if the underlying file has been deleted is OK.
    }
  }
}
