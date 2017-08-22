package me.jairam.schema

import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}

import scala.util.control.NonFatal

object Builder {

  def buildSchema(dataSchema: Iterable[DataSchema],
                  recordName: String): Either[InvalidSchemaError, Schema] = {

    val builder = SchemaBuilder.record(recordName).fields()

    try {
      val schema =
        dataSchema.foldLeft(builder) {
          case (bldr, schm) =>
            schm.dataType match {
              case INT =>
                if (schm.isNullable) bldr.optionalInt(schm.columnName)
                else bldr.requiredInt(schm.columnName)
              case LONG =>
                if (schm.isNullable) bldr.optionalLong(schm.columnName)
                else bldr.requiredLong(schm.columnName)
              case DECIMAL =>
                if (schm.isNullable) {
                  bldr
                    .name(schm.columnName)
                    .`type`(optionalDecimal(schm.precision, schm.scale))
                    .withDefault(null)
                } else {
                  bldr
                    .name(schm.columnName)
                    .`type`(requiredDecimal(schm.precision, schm.scale))
                    .noDefault()
                }
              case DOUBLE =>
                if (schm.isNullable) bldr.optionalDouble(schm.columnName)
                else bldr.requiredDouble(schm.columnName)
              case BOOLEAN =>
                if (schm.isNullable) bldr.optionalBoolean(schm.columnName)
                else bldr.requiredBoolean(schm.columnName)
              case STRING =>
                if (schm.isNullable) bldr.optionalString(schm.columnName)
                else bldr.requiredString(schm.columnName)
              case NULL =>
                throw new Exception("NULL datatype is not supported.")
            }
        } endRecord ()
      Right(schema)
    } catch {
      case NonFatal(e) =>
        Left(InvalidSchemaError(e.getMessage))
    }
  }

  private def optionalDecimal(precision: Int, scale: Int): Schema = {
    import java.util

    val decimalSchema =
      LogicalTypes
        .decimal(precision, scale)
        .addToSchema(Schema.create(Schema.Type.BYTES))

    val union = new util.ArrayList[Schema]()
    union.add(Schema.create(Schema.Type.NULL))
    union.add(decimalSchema)
    Schema.createUnion(union)
  }

  private def requiredDecimal(precision: Int, scale: Int): Schema = {
    LogicalTypes
      .decimal(precision, scale)
      .addToSchema(Schema.create(Schema.Type.BYTES))
  }
}
