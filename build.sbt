name := "csv-avro-converter"

version := "1.0"

scalaVersion := "2.12.1"

libraryDependencies ++= Seq (
  "com.opencsv" % "opencsv" % "3.8"
  , "org.apache.avro" % "avro" % "1.8.2"
  , "org.rogach" %% "scallop" % "3.1.0"
)