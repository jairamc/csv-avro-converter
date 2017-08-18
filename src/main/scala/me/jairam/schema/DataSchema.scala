package me.jairam.schema

case class DataSchema(
    columnName: String,
    dataType: DataType,
    maxLength: Int = 100,
    precision: Int = 10,
    scale: Int = 0,
    nullable: Int
) {
  require(Set(0, 1).contains(nullable))

  def isNullable = nullable == 1
}
