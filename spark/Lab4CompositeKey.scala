package ru.philit.bigdata.vsu.spark.exercise

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import ru.philit.bigdata.vsu.spark.exercise.domain.{Customer, Order, Product}

object Lab4CompositeKey extends App{


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

  def aggregateOrders(acc: (Int,Int), record: (Int, Int)):(Int,Int) = acc match{
    case(numOfProducts, count) => (numOfProducts + record._1,count)
  }

  orders.map{
    case Order(customerID,_,_, numberOfProduct,_,_) => (customerID,(numberOfProduct,0))
  }.reduceByKey(aggregateOrders).join( customers.map{
    case Customer(id,name,_,_,_) => (id,name)
  }).cartesian(products.map{
    case Product(id,name,price,_) => (id,(name,price))
  }).map{
    case((_,((prodnum,_),custname)),(_,(prodname,prodprice))) => (custname, prodname, prodnum*prodprice)
  }.repartition(1).saveAsTextFile(Parameters.EXAMPLE_OUTPUT_PATH + "lab4")
//((4,((1400,0),Anastasia)),(1,(Apple iPhone 7,45990.0)))

  //
  /*
 * Lab4 - пример использования cartesian и join по составному ключу
 * Расчитать кто и на какую сумму купил определенного товара за всё время
 * Итоговое множество содержит поля: customer.name, product.name, order.numberOfProduct * product.price
 *
 * 1. Создать экземпляр класса SparkConf
 * 2. Установить мастера на local[*] и установить имя приложения
 * 3. Создать экземпляр класса SparkContext, используя объект SparkConf
 * 4. Загрузить в RDD файлы src/test/resources/input/order
 * 5. Используя класс [[ru.phil_it.bigdata.entity.Order]], распарсить RDD
 * 6. Выбрать ключ (customerID, productID), значение (numberOfProduct)
 * 7. Загрузить в RDD файлы src/test/resources/input/product
 * 8. Используя класс [[ru.phil_it.bigdata.entity.Product]], распарсить RDD
 * 9. Загрузить в RDD файлы src/test/resources/input/customer
 * 10. Используя класс [[ru.phil_it.bigdata.entity.Customer]], распарсить RDD
 * 11. Выполнить перекрестное соединение RDD из п.8 и п.10
 * 12. Выбрать ключ (customer.id, product.id), значение (customer.name, product.name, prodcut.price)
 * 13. Выполнить левое соединение RDD из п.6 и п.13
 * 14. Поставить заглушку на результат соединения для левой таблицы ("default", "default", 0d)
 * 15. Выбрать поля custemer.name, product.name, order.numberOfProduct * product.price
 * 16. Вывести результат или записать в директорию src/test/resources/output/lab4
 * */
}
