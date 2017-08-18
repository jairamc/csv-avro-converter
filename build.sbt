lazy val commonSettings = Seq(
  organization := "me.me.jairam",
  version := "1.0",
  scalaVersion := "2.12.3"
)

lazy val lib = (project in file("lib"))
    .settings(
      name := "csv-avro-converter-lib",
      commonSettings,
      libraryDependencies ++= Seq (
        "com.opencsv" % "opencsv" % "3.8"
        , "org.apache.avro" % "avro" % "1.8.2"
      )
    )

lazy val cli = (project in file("cli"))
    .dependsOn(lib)
    .settings(
      name := "csv-avro-converter-cli",
      commonSettings,
      libraryDependencies += "org.rogach" %% "scallop" % "3.1.0"
    )

