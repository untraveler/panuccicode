package ru.philit.bigdata.vsu.spark.exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import ru.philit.bigdata.vsu.spark.exercise.Lab1ReduceByKey.{aggregateOrders, orders}
import ru.philit.bigdata.vsu.spark.exercise.domain.Order

object Lab5GroupBy extends App{



  Logger.getLogger("org").setLevel(Level.DEBUG)
  Logger.getLogger("netty").setLevel(Level.DEBUG)

  val sparkConf: SparkConf = new SparkConf()
    .setAppName("spark-example")
    .setMaster("local[*]")
  val sc = new SparkContext(sparkConf)
  val params: Parameters.type = Parameters

  def aggregateOrders(acc: (Int,Int), record: (Int, Int)):(Int,Int) = acc match{
    case(numOfProducts, count) => (numOfProducts + record._1,count +1)
  }

  val orders:RDD[Order] = sc.textFile(Parameters.path_order).map(str => Order(str)).filter(_.status == "delivered")

  orders.map{
    case Order(customerID,_,_, numberOfProduct,_,_) => (customerID,(numberOfProduct,1))
  }.reduceByKey(aggregateOrders).map {
    case (custId, (numOfProducts, count)) => (custId, numOfProducts, count, numOfProducts/count)
  }.repartition(1).saveAsTextFile(Parameters.EXAMPLE_OUTPUT_PATH + "lab5")


  //
  /*
 * Lab5 - пример использования groupByKey
 * Определить средний объем заказа, за всё время, для каждого заказчика
 * Итоговое множество содержит поля: order.customerID, sum(order.numberOfProduct),
 * count(order.numberOfProduct), sum(order.numberOfProduct) / count(order.numberOfProduct)
 *
 * 1. Создать экземпляр класса SparkConf
 * 2. Установить мастера на local[*] и установить имя приложения
 * 3. Создать экземпляр класса SparkContext, используя объект SparkConf
 * 4. Загрузить в RDD файд src/test/resources/input/order
 * 5. Используя класс [[ru.phil_it.bigdata.entity.Order]], распарсить строки в RDD
 * 6. Выбрать только те транзакции у которых статус delivered
 * 7. Выбрать ключ (customerID), значение (numberOfProducts)
 * 8. Выполнить группировку по ключу
 * 9. Посчитать сумму по значению и разделить на размер коллекции
 * 10. Вывести результат или записать в директорию src/test/resources/output/lab5
 * */
}
