package me.jairam.csv

import java.io.File

import me.jairam.schema.{
  BOOLEAN,
  DECIMAL,
  DOUBLE,
  DataSchema,
  INT,
  LONG,
  STRING
}
import org.scalatest.{FlatSpec, Matchers}

class CsvReaderSpec extends FlatSpec with Matchers {

  val validCsvDir = "schema_infer/valid_csv"
  val validInput =
    getClass.getClassLoader.getResource(s"$validCsvDir/input.csv").toURI
  val validCustomOptsInput =
    getClass.getClassLoader.getResource(s"$validCsvDir/custom_opts.csv").toURI
  val validCustomEscapeInput = getClass.getClassLoader
    .getResource(s"$validCsvDir/custom_escape_char.csv")
    .toURI
  val invalidCsv = getClass.getClassLoader
    .getResource("schema_infer/invalid_csv/png_in_disguise.csv")
    .toURI

  "CsvReader" should "infer schema correctly with default options" in {
    val reader = new CsvReader(new File(validInput))
    val schema = reader.inferSchema()

    val expected =
      Seq(
        DataSchema(columnName = "id",
                   dataType = DECIMAL,
                   precision = 32,
                   nullable = 1), // Check Decimal inference
        DataSchema(columnName = "firstname", dataType = STRING, nullable = 0), // Check String inference
        DataSchema(columnName = "lastname", dataType = STRING, nullable = 0),
        DataSchema(columnName = "account", dataType = STRING, nullable = 0),
        DataSchema(columnName = "balance", dataType = LONG, nullable = 0), // Check Long inference
        DataSchema(columnName = "active", dataType = BOOLEAN, nullable = 0), // Check Boolean inference
        DataSchema(columnName = "credit", dataType = DOUBLE, nullable = 1), // Check Float inference
        DataSchema(columnName = "branch", dataType = INT, nullable = 0), // Check Int inference
        DataSchema(columnName = "notes", dataType = STRING, nullable = 1) // Check that completely empty fields get marked as String
      )

    schema match {
      case Right(s) =>
        s should contain theSameElementsInOrderAs expected

      case Left(err) =>
        fail(err.msg)
    }
  }

  it should "infer schema correctly for delimiter as pipe character" in {
    val reader = new CsvReader(new File(validCustomOptsInput),
                               separator = '|',
                               quoteChar = '`')
    val schema = reader.inferSchema()

    val expected =
      Seq(
        DataSchema(columnName = "id", dataType = INT, nullable = 0),
        DataSchema(columnName = "name", dataType = STRING, nullable = 0),
        DataSchema(columnName = "age", dataType = INT, nullable = 0),
        DataSchema(columnName = "quote", dataType = STRING, nullable = 0)
      )

    schema match {
      case Right(s) =>
        s should contain theSameElementsInOrderAs expected

      case Left(err) =>
        fail(err.msg)
    }
  }

  it should "read csv with custom escape character" in {
    val reader =
      new CsvReader(new File(validCustomEscapeInput), escapeChar = '$')

    val expected =
      "id\nname\nage\nquote\n1\nhulk\n45\nHULK IS NOT AFRAID,...\"HULK IS STRONGEST ONE THERE IS!!!"
    reader.rows() match {
      case Right(rows) =>
        rows.flatten.mkString("\n") shouldBe expected

      case Left(err) =>
        fail(err.msg)
    }
  }

  it should "infer schema correctly for custom escape character" in {
    val reader =
      new CsvReader(new File(validCustomEscapeInput), escapeChar = '$')
    val schema = reader.inferSchema()

    val expected =
      Seq(
        DataSchema(columnName = "id", dataType = INT, nullable = 0),
        DataSchema(columnName = "name", dataType = STRING, nullable = 0),
        DataSchema(columnName = "age", dataType = INT, nullable = 0),
        DataSchema(columnName = "quote", dataType = STRING, nullable = 0)
      )

    schema match {
      case Right(s) =>
        s should contain theSameElementsInOrderAs expected

      case Left(err) =>
        fail(err.msg)
    }
  }

  "CsvReader" should "throw InvalidFileException if file is not csv or text file" in {
    val reader = new CsvReader(new File(invalidCsv))
    reader.inferSchema() match {
      case Left(err) =>
        err shouldBe a[CsvError]

      case Right(_) =>
        throw new Exception("This should not work")
    }
  }
}
