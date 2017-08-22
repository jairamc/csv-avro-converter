package me.jairam.schema

import java.io.File

import org.apache.avro.Schema
import org.scalatest.{FlatSpec, Matchers}

class BuilderSpec extends FlatSpec with Matchers {

  "Builder" should "build valid avro schemas from intermediate representation" in {
    val expectedAvsc = getClass.getClassLoader.getResource("avro/expected.avsc").toURI
    val expectedSchema = new Schema.Parser().parse(new File(expectedAvsc))

    val internal =
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

    Builder.buildSchema(internal, "customers") match {
      case Right(s) => s shouldEqual expectedSchema
      case Left(f) => fail(f.msg)
    }

  }

}
