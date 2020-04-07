package ru.philit.bigdata.vsu.AviaInfo


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import ru.philit.bigdata.vsu.AviaInfo.domain.{Airline, Airports, Countries, Route}


object AviaInfoParameters {

  val POPULATION_DATASET_PATH = ""
  val EXAMPLE_OUTPUT_PATH = ".\\spark_output\\"


  val path_airlines = ".\\datasource\\avia\\airlines\\*"
  val path_airports = ".\\datasource\\avia\\airports\\*"
  val path_countries = ".\\datasource\\avia\\countries\\*"
  val path_routes = ".\\datasource\\avia\\routes\\*"

  val table_airlines = "airlines"
  val table_airport = "airport"
  val table_country = "country"
  val table_route = "route"



  private def createTable(name: String, structType: StructType, path: String, delimiter: String = ",")
                         (implicit spark: SparkSession): Unit = {
    spark.read
      .format("com.databricks.spark.csv")
      //.option("inferSchema", "true")
      .options(
        Map(
          "delimiter" -> delimiter,
          "nullValue" -> "\\N"
        )
      ).schema(structType).load(path).createOrReplaceTempView(name)
  }

  def initTables(implicit spark: SparkSession): Unit = {
    createTable(AviaInfoParameters.table_airlines, Airline.structType, AviaInfoParameters.path_airlines)
    createTable(AviaInfoParameters.table_airport, Airports.structType, AviaInfoParameters.path_airports)
    createTable(AviaInfoParameters.table_country, Countries.structType, AviaInfoParameters.path_countries)
    createTable(AviaInfoParameters.table_route, Route.structType, AviaInfoParameters.path_routes)
  }

}
