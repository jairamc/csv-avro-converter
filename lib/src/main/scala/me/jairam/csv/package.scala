package me.jairam

package object csv {

  trait CsvError {
    def msg: String
  }

  case class DataError(msg: String) extends CsvError

  case class IOError(msg: String) extends CsvError

}
