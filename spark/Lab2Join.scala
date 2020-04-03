package ru.philit.bigdata.vsu.spark.exercise




import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ru.philit.bigdata.vsu.spark.exercise.domain.{Order, Product}


object Lab2Join extends App {


  Logger.getLogger("org").setLevel(Level.DEBUG)
  Logger.getLogger("netty").setLevel(Level.DEBUG)

  val sparkConf: SparkConf = new SparkConf()
    .setAppName("spark-example")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val params: Parameters.type = Parameters



  val orders:RDD[Order] = sc.textFile(Parameters.path_order).map(str => Order(str)).filter(_.status != "error")
  val products:RDD[Product] = sc.textFile(Parameters.path_product).map(str1 => Product(str1))


    orders.map{
    case Order(_,_,productID,numberOfProduct,_,_) => (productID,numberOfProduct)
  }.leftOuterJoin(products.map{
      case Product(id,name,_,_) => (id,name)
    }).filter{
      case(_,(numberOfProduct, _)) => numberOfProduct ==  0
    }.repartition(1).saveAsTextFile(Parameters.EXAMPLE_OUTPUT_PATH + "lab2")

  sc.stop()


  /*
 * Lab2 - пример использования leftOuterJoin
 * Определить продукты, которые ни разу не были заказаны
 * */
}
