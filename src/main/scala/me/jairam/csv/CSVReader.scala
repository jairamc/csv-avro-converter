package me.jairam.csv

import java.io.{File, FileReader}
import java.math.BigDecimal

import com.opencsv
import com.opencsv.CSVParser

import collection.JavaConverters.asScalaIteratorConverter

import scala.util.control.Exception._

class CSVReader(
  file: File
, separator: Char = CSVParser.DEFAULT_SEPARATOR
, quoteChar: Char = CSVParser.DEFAULT_QUOTE_CHARACTER
, escapeChar: Char = CSVParser.DEFAULT_ESCAPE_CHARACTER
) {
  private val reader =
    new opencsv.CSVReader(new FileReader(file), separator, quoteChar, escapeChar)

  def rows(): Iterator[Array[String]] = reader.iterator().asScala

  def inferSchema(): Either[CsvError, Array[DataSchema]] = {
    val rows = reader.iterator().asScala
    if (!rows.hasNext) return Left(DataError("Empty file"))
    val headers = rows.next()

    // Start with the assumption that the field is not nullable. Set the type to NullType as it will be inferred later.
    // At all points in this file, nullable must be explicitly set
    val types = for (header <- headers) yield DataSchema(header, NULL, nullable = 0)

    val schemaWithNullTypes =
      rows.foldLeft[Array[DataSchema]](types) {
        case (typesSoFar, row) =>
          mergeRowTypes(typesSoFar, inferRowType(typesSoFar, row))
      }

    // If at the end of inference, the field still stays as NullType change type to String type and set nullable to true
    Right(
      for (ds <- schemaWithNullTypes) yield {
        if (ds.dataType == NULL) ds.copy(dataType = STRING, nullable = 1) else ds
      }
    )
  }

  private def inferRowType(rowSoFar: Array[DataSchema], next: Array[String]): Array[DataSchema] = {

    val inferred = rowSoFar.clone()

    var i = 0
    while (i < math.min(rowSoFar.length, next.length)) {  // May have columns on right missing.
      inferred(i) = inferField(rowSoFar(i), next(i))
      i+=1
    }
    inferred
  }

  private def mergeRowTypes(first: Array[DataSchema], second: Array[DataSchema]): Array[DataSchema] = {
    first.zip(second).map { case (a, b) =>
      findTightestCommonType(a, b).getOrElse(a.copy(dataType = NULL, nullable = 1))
    }
  }

  /**
    * Infer type of string field. Given known type Double, and a string "1", there is no
    * point checking if it is an Int, as the final type must be Double or higher.
    */
  private def inferField(typeSoFar: DataSchema, field: String): DataSchema = {
    if (Option(field).forall(_.isEmpty)) {
      typeSoFar.copy(dataType = NULL)
    } else {
      typeSoFar.dataType match {
        case NULL => tryParseInteger(typeSoFar, field)
        case INT => tryParseInteger(typeSoFar, field)
        case LONG => tryParseLong(typeSoFar, field)
        case DECIMAL => tryParseDecimal(typeSoFar, field)
        case DOUBLE => tryParseDouble(typeSoFar, field)
        case BOOLEAN => tryParseBoolean(typeSoFar, field)
        case STRING => typeSoFar
        case other: DataType =>
          throw new UnsupportedOperationException(s"Unexpected data type $other")
      }
    }
  }

  private def tryParseInteger(typeSoFar: DataSchema, field: String): DataSchema = {
    if ((allCatch opt field.toInt).isDefined) {
      typeSoFar.copy(dataType = INT)
    } else {
      tryParseLong(typeSoFar, field)
    }
  }

  private def tryParseLong(typeSoFar: DataSchema, field: String): DataSchema = {
    if ((allCatch opt field.toLong).isDefined) {
      typeSoFar.copy(dataType = LONG)
    } else {
      tryParseDecimal(typeSoFar, field)
    }
  }

  private def tryParseDecimal(typeSoFar: DataSchema, field: String): DataSchema = {
    val decimalTry = allCatch opt {
      // `BigDecimal` conversion can fail when the `field` is not a form of number.
      val bigDecimal = new BigDecimal(field)
      // Because many other formats do not support decimal, it reduces the cases for
      // decimals by disallowing values having scale (eg. `1.1`).
      if (bigDecimal.scale <= 0) {
        // `DecimalType` conversion can fail when
        //   1. The precision is bigger than 38.
        //   2. scale is bigger than precision.
        typeSoFar.copy(dataType = DECIMAL, precision = bigDecimal.precision, scale = bigDecimal.scale)
      } else {
        tryParseDouble(typeSoFar, field)
      }
    }
    decimalTry.getOrElse(tryParseDouble(typeSoFar, field))
  }

  private def tryParseDouble(typeSoFar: DataSchema, field: String): DataSchema = {
    if ((allCatch opt field.toDouble).isDefined) {
      typeSoFar.copy(dataType = DOUBLE)
    } else {
      tryParseBoolean(typeSoFar, field)
    }
  }

  private def tryParseBoolean(typeSoFar: DataSchema, field: String): DataSchema = {
    if ((allCatch opt field.toBoolean).isDefined) {
      typeSoFar.copy(dataType = BOOLEAN)
    } else {
      typeSoFar.copy(dataType = STRING)
    }
  }


  private val numericPrecedence: IndexedSeq[DataType] = IndexedSeq(
    BYTE,
    SHORT,
    INT,
    LONG,
    DOUBLE)

  private def findTightestCommonType(a: DataSchema, b: DataSchema): Option[DataSchema] = {
    require(a.columnName == b.columnName, s"Cannot find common type for different columns. ${a.columnName} != ${b.columnName}")
    (a.dataType, b.dataType) match {
      // We started off with all fields being NullType
      case (NULL, NULL) => Some(a.copy(nullable = 1)) // Two NullTypes means its nullable
      case (t1, t2) if t1 == t2 => Some(a) // They are same, but not both NullType
      case (_, NULL) => Some(a.copy(nullable = 1)) // Null field found, is nullable
      // Was previously NullType, set to new Type. Because we initialized `nullable` to false, this field will be marked as not nullable until futher inference
      case (NULL, _) => Some(b)
      case (STRING, _) => Some(a.copy(dataType = STRING))
      case (_, STRING) => Some(a.copy(dataType = STRING))

      // Promote numeric types to the highest of the two and all numeric types to unlimited decimal
      case (t1, t2) if Seq(t1, t2).forall(numericPrecedence.contains) =>
        val index = numericPrecedence.lastIndexWhere(t => t == t1 || t == t2)
        Some(a.copy(dataType = numericPrecedence(index)))

      // Double support larger range than fixed decimal, DecimalType.Maximum should be enough
      // in most case, also have better precision.
      case (DOUBLE, DECIMAL) | (DECIMAL, DOUBLE) =>
        Some(a.copy(dataType = DOUBLE))

      case (DECIMAL, DECIMAL) =>
        val scale = math.max(a.scale, b.scale)
        val range = math.max(a.precision - a.scale, b.precision - b.scale)
        if (range + scale > 38) {
          // DecimalType can't support precision > 38
          Some(a.copy(dataType = DOUBLE))
        } else {
          Some(a.copy(dataType = DECIMAL, precision = range + scale, scale = scale))
        }

      case _ => None
    }
  }
}

trait CsvError{
  def msg: String
}

case class DataError(msg: String) extends CsvError
