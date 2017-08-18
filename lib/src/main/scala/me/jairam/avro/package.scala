package me.jairam

package object avro {

  sealed trait AvroError {
    def msg: String
  }

  case class SchemaError(msg: String) extends AvroError

}
