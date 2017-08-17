package me.jairam.avro

import java.nio.ByteBuffer
import java.io.File

import me.jairam.csv._
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}

import collection.JavaConverters.asScalaBufferConverter

import AvroWriter._

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
      writer.close() // Closing the writer even if the underlying file has been deleted is OK.
    }
  }

  def buildSchema(dataSchema: Iterable[DataSchema], recordName: String, namespace: String): Schema = {
    val builder = SchemaBuilder.record(recordName).namespace(namespace).fields()
    dataSchema.foldLeft(builder) {
      case (bldr, schm) =>
        schm.dataType match {
          case INT =>
            if (schm.isNullable) bldr.optionalInt(schm.columnName) else bldr.requiredInt(schm.columnName)
          case LONG =>
            if (schm.isNullable) bldr.optionalLong(schm.columnName) else bldr.requiredLong(schm.columnName)
          case DECIMAL =>
            if (schm.isNullable) {
              bldr.name(schm.columnName).`type`(optionalDecimal(schm.precision, schm.scale)).withDefault(null)
            } else {
              bldr.name(schm.columnName).`type`(requiredDecimal(schm.precision, schm.scale)).noDefault()
            }
          case DOUBLE =>
            if (schm.isNullable) bldr.optionalDouble(schm.columnName) else bldr.requiredDouble(schm.columnName)
          case BOOLEAN =>
            if (schm.isNullable) bldr.optionalBoolean(schm.columnName) else bldr.requiredBoolean(schm.columnName)
          case STRING =>
            if (schm.isNullable) bldr.optionalString(schm.columnName) else bldr.requiredString(schm.columnName)
        }
    } endRecord()
  }

  private def optionalDecimal(precision: Int, scale: Int): Schema = {
    import java.util

    val decimalSchema = LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES))

    val union = new util.ArrayList[Schema]()
    union.add(Schema.create(Schema.Type.NULL))
    union.add(decimalSchema)
    Schema.createUnion(union)
  }

  private def requiredDecimal(precision: Int, scale: Int): Schema = {
    LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES))
  }
}

object AvroWriter {
  implicit class RichAvroField(field: Schema.Field) {
    def cast(value: String): Any = {
      getType() match {
        case "int" => if (value.isEmpty) null else value.toInt
        case "long" => if (value.isEmpty) null else value.toLong
        case "float" => if (value.isEmpty) null else value.toFloat
        case "double" => if (value.isEmpty) null else value.toDouble
        case "boolean" => if (value.isEmpty) null else value.toBoolean
        case "decimal" => if (value.isEmpty) null else ByteBuffer.wrap(BigDecimal(value).toBigInt().toByteArray)
        case _ => value
      }
    }

    private def getType(): String = {
      field.schema().getType match {
        case Schema.Type.UNION => // Optional field
          val types = field.schema().getTypes.ensuring(_.size == 2, "Unsupported Avro Schema")
          val s = types.asScala.filter(_.getName != "null").ensuring(_.size == 1, "Unsupported avro schema. Fields can only have one type.").head
          getFieldName(s)

        case _ => getFieldName(field.schema())
      }
    }

    private def getFieldName(schema: Schema): String = {
      try {
        LogicalTypes.fromSchema(schema).getName
      } catch {
        case _: Exception =>
          schema.getName
      }
    }
  }
}


