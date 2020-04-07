package ru.philit.bigdata.vsu.AviaInfo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ru.philit.bigdata.vsu.AviaInfo.domain.{Airline, Airports, Route}

object AviaInfo extends App{



  Logger.getLogger("org").setLevel(Level.DEBUG)
  Logger.getLogger("netty").setLevel(Level.DEBUG)



  val sparkConf = new SparkConf()
    .setAppName("aviainfo")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val params: AviaInfoParameters.type = AviaInfoParameters

  val airport:RDD[Airports] = sc.textFile(AviaInfoParameters.path_airports).map(str => Airports(str))
  val airline:RDD[Airline] = sc.textFile(AviaInfoParameters.path_airlines).map(str => Airline(str))
  val routes:RDD[Route] = sc.textFile(AviaInfoParameters.path_routes).map(str => Route(str))





  def aggregateOrders(acc: (Int,Int), record: (Int, Int)) = acc match{
    case(numOfProducts, count) => (numOfProducts + record._1,count)
  }

  airport.map{
    case Airports(_,name,city,country,iata,_,_,_,_,_,_,_,_,_) => ((iata,name),(country,city))
  }.repartition(1).saveAsTextFile(AviaInfoParameters.EXAMPLE_OUTPUT_PATH + "aviainfo")
  airline.map{
   case Airline(_,name,_,_,_,_,country,_) => (name, country)
  }

  routes.map{
    case Route(_,_,source_airport,_,target_airport,_,_,_,_) => (source_airport,target_airport)
  }


}
//