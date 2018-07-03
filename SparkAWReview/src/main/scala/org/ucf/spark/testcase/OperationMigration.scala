package org.ucf.spark.testcase

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.ucf.spark.reviewData

trait OperationMigration {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AmazonReviews_OM")
      .getOrCreate()

    /*
       scala> review.printSchema
       root
       |-- asin: string (nullable = true)
       |-- helpful: array (nullable = true)
       |    |-- element: long (containsNull = true)
       |-- overall: double (nullable = true)
       |-- reviewText: string (nullable = true)
       |-- reviewTime: string (nullable = true)
       |-- reviewerID: string (nullable = true)
       |-- reviewerName: string (nullable = true)
       |-- summary: string (nullable = true)
       |-- unixReviewTime: long (nullable = true)
    */
    val reviewRDD:RDD[Row] = spark.read.json(reviewData).rdd
    val words = reviewRDD.filter(_.getDouble(2) > 4).map(row => {
      val asin = row.getString(0)
      val reviewText = row.getString(3)
      val count = reviewText.split(" ").length
      (asin,count)
    })
    val aggData = words.groupByKey()
    println("The number of data is: " + aggData.count())
    spark.close()
  }
}


trait OperationMigrationOptimzied {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AmazonReviews_OM_Optimzied")
      .getOrCreate()

    /*
         scala> review.printSchema
         root
         |-- asin: string (nullable = true)
         |-- helpful: array (nullable = true)
         |    |-- element: long (containsNull = true)
         |-- overall: double (nullable = true)
         |-- reviewText: string (nullable = true)
         |-- reviewTime: string (nullable = true)
         |-- reviewerID: string (nullable = true)
         |-- reviewerName: string (nullable = true)
         |-- summary: string (nullable = true)
         |-- unixReviewTime: long (nullable = true)
      */
    val reviewRDD:RDD[Row] = spark.read.json(reviewData).rdd
    val words = reviewRDD.filter(_.getDouble(2) > 4).map(row => {
      val asin = row.getString(0)
      val reviewText = row.getString(3)
      val count = reviewText.split(" ").length
      (asin,count)
    })
    val aggData = words.reduceByKey(_ + _)
    println("The number of data is: " + aggData.count())
    spark.close()
  }
}