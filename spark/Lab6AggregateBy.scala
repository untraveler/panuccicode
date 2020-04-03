package ru.philit.bigdata.vsu.spark.exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ru.philit.bigdata.vsu.spark.exercise.domain.Order

object Lab6AggregateBy extends App{



  Logger.getLogger("org").setLevel(Level.DEBUG)
  Logger.getLogger("netty").setLevel(Level.DEBUG)

  val sparkConf: SparkConf = new SparkConf()
    .setAppName("spark-example")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val params: Parameters.type = Parameters

  case class Result(uniqProducts: Set[Int], uniqNumOfProducts: Seq[Int], sumNumOfProduct: Int)
  val orders:RDD[Order] = sc.textFile(Parameters.path_order).map(str => Order(str)).filter(_.status == "delivered")


  orders.map{
    case Order(customerID,_,productID,numberOfProduct,_,_) => (customerID,productID,numberOfProduct)
  }.map(v => (v._1, (v._2,v._3))).aggregateByKey(0)()
  Result(orders.map{
    case Order(customerID,_,_,_,_,_) => customerID
  }.collect().toSet,orders.map{
    case Order(_,_,productID,_,_,_) => productID
  }.collect().toSeq,orders.map{
    case Order(_,_,_,numberOfProduct,_,_) => numberOfProduct
  }.collect().head)
  //.repartition(1).saveAsTextFile(Parameters.EXAMPLE_OUTPUT_PATH + "lab5")
  /*
   * Lab6 - пример использования aggregateByKey
   * Определить кол-во уникальных заказов, максимальный объем заказа,
   * минимальный объем заказа, общий объем заказа за всё время
   * Итоговое множество содержит поля: order.customerID, count(distinct order.productID),
   * max(order.numberOfProduct), min(order.numberOfProduct), sum(order.numberOfProduct)
   *
   * 1. Создать экземпляр класса SparkConf
   * 2. Установить мастера на local[*] и установить имя приложения
   * 3. Создать экземпляр класса SparkContext, используя объект SparkConf
   * 4. Загрузить в RDD файл src/test/resources/input/order
   * 5. Используя класс [[ru.phil_it.bigdata.entity.Order]], распарсить строки в RDD
   * 6. Выбрать только те транзакции у которых статус delivered
   * 7. Выбрать ключ (customerID), значение (productID, numberOfProducts)
   * 8. Создать кейс класс Result(uniqProducts: Set[Int], uniqNumOfProducts: Seq[Int], sumNumOfProduct: Int)
   * 9. Создать аккумулятор с начальным значением
   * 10. Создать анонимную функцию для заполнения аккумулятора
   * 11. Создать анонимную функцию для слияния аккумуляторов
   * 12. Выбрать id заказчика, размер коллекции uniqProducts,
   *   максимальное и минимальное значение из uniqNumOfProducts и sumNumOfProduct
   * 13. Вывести результат или записать в директорию src/test/resources/output/lab6
   * */
}
