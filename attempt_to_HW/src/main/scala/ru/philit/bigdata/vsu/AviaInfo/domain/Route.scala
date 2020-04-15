package ru.philit.bigdata.vsu.AviaInfo.domain

import org.apache.spark.sql.types._

object Route extends Enumeration {


  private val DELIMITER = "\\t"
  val AIRLINE, AIRLINE_ID, SOURCE_AIRPORT, SOURCE_AIRPORT_ID, TARGET_AIRPORT, TARGET_AIRPORT_ID, STOPS, EQUIPMENT = Value
  val structType = StructType(
    Seq(
      StructField(AIRLINE.toString, StringType),
      StructField(AIRLINE_ID.toString, IntegerType),
      StructField(SOURCE_AIRPORT.toString, StringType),
      StructField(SOURCE_AIRPORT_ID.toString, IntegerType),
      StructField(TARGET_AIRPORT.toString, StringType),
      StructField(TARGET_AIRPORT_ID.toString, IntegerType),
      StructField(STOPS.toString, IntegerType),
      StructField(EQUIPMENT.toString, StringType)
    )
  )
  def apply(row: String): Route= {
    val array = row.split(DELIMITER, -1)
    Route(
      array(AIRLINE.id),
      array(AIRLINE_ID.id).toInt,
      array(SOURCE_AIRPORT.id),
      array(SOURCE_AIRPORT_ID.id).toInt,
      array(TARGET_AIRPORT.id),
      array(TARGET_AIRPORT_ID.id).toInt,
      array(STOPS.id).toInt,
      array(EQUIPMENT.id)

    )

  }
}
case class Route(
                airline:String,
                airlineID:Int,
                source_airport:String,
                source_airport_id:Int,
                target_airport:String,
                target_airport_id:Int,
                stops:Int,
                equipment:String
                )