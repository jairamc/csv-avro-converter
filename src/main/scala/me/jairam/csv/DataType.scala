package me.jairam.csv

sealed abstract class DataType(val name: String)

object DataType {
  def fromName(name: String): DataType = {
    name.toLowerCase match {
      case NULL.name => NULL
      case BYTE.name => BYTE
      case INT.name => INT
      case LONG.name => LONG
      case SHORT.name => SHORT
      case DECIMAL.name => DECIMAL
      case DOUBLE.name => DOUBLE
      case BOOLEAN.name => BOOLEAN
      case STRING.name => STRING
      case t => throw new RuntimeException(s"Unknown data type $t")
    }
  }
}

case object NULL extends DataType("null")
case object BYTE extends DataType("byte")
case object INT extends DataType("int")
case object LONG extends DataType("long")
case object SHORT extends DataType("short")

// Decimal is only used for very large integers
// As to why not BigInt, Avro does not support it and Spark doesn't deal very well with it either.
case object DECIMAL extends DataType("decimal")

case object DOUBLE extends DataType("double")
case object BOOLEAN extends DataType("boolean")
case object STRING extends DataType("string")
