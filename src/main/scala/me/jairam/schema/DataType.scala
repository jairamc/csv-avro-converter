package me.jairam.schema

sealed abstract class DataType(val name: String)

case object NULL extends DataType("null")
case object INT extends DataType("int")
case object LONG extends DataType("long")

// Decimal is only used for very large integers
// As to why not BigInt, Avro does not support it and Spark doesn't deal very well with it either.
case object DECIMAL extends DataType("decimal")

case object DOUBLE extends DataType("double")
case object BOOLEAN extends DataType("boolean")
case object STRING extends DataType("string")
