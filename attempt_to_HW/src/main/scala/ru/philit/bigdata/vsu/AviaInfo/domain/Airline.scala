package ru.philit.bigdata.vsu.AviaInfo.domain


import org.apache.spark.sql.types._



object Airline extends Enumeration{
  private val DELIMITER = "\\t"
  val ID, NAME, ALIAS, IATA, ICAO, CALLSIGN, COUNTRY, ACTIVE   = Value

  val structType = StructType(
    Seq(
      StructField(ID.toString, IntegerType),
      StructField(NAME.toString, StringType),
      StructField(ALIAS.toString, StringType),
      StructField(IATA.toString, StringType),
      StructField(ICAO.toString, StringType),
      StructField(CALLSIGN.toString, StringType),
      StructField(COUNTRY.toString, StringType),
      StructField(ACTIVE.toString, StringType)
    )
  )


  def apply(row: String): Airline = {
    val array = row.split(DELIMITER, -1)
    Airline(
      array(ID.id).toInt,
      array(NAME.id),
      array(ALIAS.id),
      array(IATA.id),
      array(ICAO.id),
      array(CALLSIGN.id),
      array(COUNTRY.id),
      array(ACTIVE.id)

    )

  }
}


case class Airline(
                  id:Int,
                  name:String,
                  alias:String,
                  iata:String,
                  icao:String,
                  callsign:String,
                  country:String,
                  active:String

                  )