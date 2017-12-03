package org.ucf.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object SparkAWReview {

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    unOptimized()
    val stopTime = System.currentTimeMillis()
    val time = stopTime-startTime
    println("The total execution time is: "+time)
  }

  def unOptimized(): Unit ={
    val spark = SparkSession.builder()
      .appName("ScalaWordCount")
      .getOrCreate()

    //val sc = spark.sparkContext
    val reviewRDD:RDD[Row] = spark.read.json("/data/amazon/review.data").toJavaRDD
    val review1 = reviewRDD.map(row => (row.getString(0),row.toString())).groupByKey().count()
    val review2 = reviewRDD.map(row => (row.getString(0),row.toString())).reduceByKey((a,b) => a+b).count()



    val review3 = reviewRDD.map(row => (row.getString(0),1)).reduceByKey((a,b) => a+b).count()



    //val count = reviewKV.groupByKey().count()
    println("The count size data is :"+ count)
    spark.close()
  }


  def optimized(): Unit ={
    val spark = SparkSession.builder()
      .appName("ScalaWordCount")
      .getOrCreate()

    //val sc = spark.sparkContext
    val reviewRDD:RDD[Row] = spark.read.json("/data/amazon/review.data").toJavaRDD
    //val metadataRDD:RDD[Row] = spark.read.json("/data/amazon/meta.data").toJavaRDD
    val reviewKV = reviewRDD.map(row => (row.getString(0),row.toString())).reduceByKey((a,b) => a+b))
    val count = reviewKV.reduceByKey((a,b) => a+b))
    println("The count size data is :"+count)
    spark.close()
  }


}
