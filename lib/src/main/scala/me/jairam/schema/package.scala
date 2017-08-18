package me.jairam

import java.nio.ByteBuffer

import org.apache.avro.{LogicalTypes, Schema}

import scala.collection.JavaConverters.asScalaBufferConverter

package object schema {

  implicit class RichAvroField(field: Schema.Field) {
    def cast(value: String): Any = {
      getType() match {
        case "int"     => if (value.isEmpty) null else value.toInt
        case "long"    => if (value.isEmpty) null else value.toLong
        case "float"   => if (value.isEmpty) null else value.toFloat
        case "double"  => if (value.isEmpty) null else value.toDouble
        case "boolean" => if (value.isEmpty) null else value.toBoolean
        case "decimal" =>
          if (value.isEmpty) null
          else ByteBuffer.wrap(BigDecimal(value).toBigInt().toByteArray)
        case _ => value
      }
    }

    private def getType(): String = {
      field.schema().getType match {
        case Schema.Type.UNION => // Optional field
          val types = field
            .schema()
            .getTypes
            .ensuring(_.size == 2, "Unsupported Avro Schema")
          val s = types.asScala
            .filter(_.getName != "null")
            .ensuring(_.size == 1,
                      "Unsupported avro schema. Fields can only have one type.")
            .head
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
