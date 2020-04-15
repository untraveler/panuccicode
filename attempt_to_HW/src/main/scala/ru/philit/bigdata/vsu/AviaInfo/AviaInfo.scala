package ru.philit.bigdata.vsu.AviaInfo

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ru.philit.bigdata.vsu.AviaInfo.domain.{Airline, Airports,Route}



object AviaInfo extends App{



  Logger.getLogger("org").setLevel(Level.DEBUG)
  Logger.getLogger("netty").setLevel(Level.DEBUG)



  val sparkConf = new SparkConf()
    .setAppName("aviainfo")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val params: AviaInfoParameters.type = AviaInfoParameters

  val airprt:RDD[Airports] = sc.textFile(AviaInfoParameters.path_airports).map(str => Airports(str))
  val airline:RDD[Airline] = sc.textFile(AviaInfoParameters.path_airlines).map(str => Airline(str))
  val routes:RDD[Route] = sc.textFile(AviaInfoParameters.path_routes).map(str => Route(str))


  def Rec(param:String):Unit ={
    airline.map{
      case Airline(id,name,_,iata,icao,_,_,_) => (id,name,iata,icao)
    }.filter(a => a._2.equals(param) || a._3.equals(param) || a._4.equals(param)).map{
      case(id,name,iata,icao) => (id,(name,iata,icao))
    }.join(routes.map{
      case Route(_,airlineID,_,_,target_airport,target_airport_id,_,_) => (airlineID,(target_airport,target_airport_id))
    }).map{
      case(_,((_,_,_),(targetAi,tarId))) => (tarId, targetAi)
    }.join(airprt.map{
      case Airports(airportId,name,city,country,_,_,_,_,_,_,_,_,_,_) => (airportId,(name,city,country))
    }).map{
      case(_,(_,(airpName,city,country))) => ((airpName,city,country),1)
    }.reduceByKey(_+_)

  }.repartition(1).saveAsTextFile(AviaInfoParameters.EXAMPLE_OUTPUT_PATH + "aviainfo")
  Rec("AstrakhanAirlines")


}
///*airline.map{
//    case Airline(id,name,_,iata,icao,_,_,_) => (id,name,iata,icao)
//  }
//
//  //.
//  airprt.map{
//    case Airports(airportId,name,city,country,iata,_,_,_,_,_,_,_,_,_) => (airportId,name,city,country,iata)
//  }
//
//  routes.map{
//    case Route(airline,airlineID,source_airport,source_airport_id,target_airport,target_airport_id,_,_) =>
//      (airline,airlineID,source_airport,source_airport_id,target_airport,target_airport_id)
//  }*/

/*.join(airprt.map{
      case Airports(airportId,name,_,country,_,_,_,_,_,_,_,_,_,_) => (airportId,(name,country))
    }).map{
      case ()
    }*/