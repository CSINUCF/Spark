package org.ucf.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkTest {
  spark =>

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkTestCase")
    val sc = new SparkContext(conf)
    val text = sc.textFile("data.txt")
    val counts = text.flatMap(line => line.split(" "))
                     .map(word => (word,1))
                     .reduceByKey(_ + _)
                     .filter({ case (word,number) => word.contains("abc") }).map(_._1).filter(_.contains("edf"))
    val reg = counts.collect()
    println("the number of element is: " + reg)
    sc.stop()
  }
}
