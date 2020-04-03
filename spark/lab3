package ru.philit.bigdata.vsu.spark.exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ru.philit.bigdata.vsu.spark.exercise.domain.{Customer, Order, Product}

object Lab3Join extends App{

  Logger.getLogger("org").setLevel(Level.DEBUG)
  Logger.getLogger("netty").setLevel(Level.DEBUG)

  val sparkConf: SparkConf = new SparkConf()
    .setAppName("spark-example")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val params: Parameters.type = Parameters


  val orders:RDD[Order] = sc.textFile(Parameters.path_order).map(str => Order(str)).filter(_.status != "error")
  val customers:RDD[Customer] = sc.textFile(Parameters.path_customer).map(str => Customer(str)).filter(_.status != "banned")
  val products:RDD[Product] = sc.textFile(Parameters.path_product).map(str1 => Product(str1))


  customers.map{
    case Customer(id,name,_,_,_) => (id,name)
  }.join(orders.map{
    case Order(customerID, _, productID, numberOfProduct, orderDate, _) => (customerID,(productID, numberOfProduct, orderDate))
  }).join(products.map{
    case Product(id,_,price,_) => (id,price)
  }).map{
    case(_,((name,(_,prodnum,ordDate)),prodprice)) => (name,prodnum,ordDate, prodprice * prodnum)
  }.repartition(1).saveAsTextFile(Parameters.EXAMPLE_OUTPUT_PATH + "lab3")

  sc.stop()
  //
  /*
   * Lab3 - пример использования join
   * Расчитать кто и сколько сделал заказов, за какие даты, на какую сумму.
   * Итоговое множество содержит поля: customer.name, order.order.orderDate, sum(order.numberOfProduct * product.price)
  */
}
