package ru.philit.bigdata.vsu.AviaInfo.domain


import java.sql.Date

import org.apache.spark.sql.types._

object Airports extends Enumeration {

  private val DELIMITER = "\\t"
  val AIRPORTID, NAME, CITY, COUNTRY, IATA, ICAO, LAT, LON, ALTITUDE, TIMEZONE, DST, TZ, TYPE, SOURCE = Value

  val structType = StructType(
    Seq(
      StructField(AIRPORTID.toString, IntegerType),
      StructField(NAME.toString, StringType),
      StructField(CITY.toString, StringType),
      StructField(COUNTRY.toString, StringType),
      StructField(IATA.toString, StringType),
      StructField(ICAO.toString, StringType),
      StructField(LAT.toString, DoubleType),
      StructField(LON.toString, DoubleType),
      StructField(ALTITUDE.toString, IntegerType),
      StructField(TIMEZONE.toString, IntegerType),
      StructField(DST.toString, StringType),
      StructField(TZ.toString, StringType),
      StructField(TYPE.toString, StringType),
      StructField(SOURCE.toString, StringType)
    )
  )

  def apply(row: String): Airports = {
    val array = row.split(DELIMITER, -1)
    Airports(
      array(AIRPORTID.id).toInt,
      array(NAME.id),
      array(CITY.id),
      array(COUNTRY.id),
      array(IATA.id),
      array(ICAO.id),
      array(LAT.id).toDouble,
      array(LON.id).toDouble,
      array(ALTITUDE.id).toInt,
      array(TIMEZONE.id).toInt,
      array(DST.id),
      array(TZ.id),
      array(TYPE.id),
      array(SOURCE.id)
    )

  }

}
case class Airports(
                   airportId :Int,
                   name:String,
                   city:String,
                   country:String,
                   iata:String,
                   icao:String,
                   lat:Double,
                   lon:Double,
                   altitude:Int,
                   timezone:Int,
                   dst:String,
                   tz:String,
                   atype:String,
                   source:String
                   )
