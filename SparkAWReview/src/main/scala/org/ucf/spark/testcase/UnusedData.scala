package org.ucf.spark.testcase

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.ucf.spark.reviewData

trait UnusedData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AmazonReviews_UD")
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
    val aggData = reviewRDD.map(
      row =>
        (row.getString(0),(row.getDouble(2),row.getString(3)))
    ).groupByKey().map({
       case (asin,values) => {
          val average = values.map(_._1).sum / values.size
          (asin,average)
        }
     }
   )



    println("The number of data is: " + aggData.count())
    spark.close()
  }
}

trait UnusedDataOptimized {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AmazonReviews_UD_Optimized")
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
    val aggData = reviewRDD.map(row => (row.getString(0),row.getDouble(2))).groupByKey().map(
      {
        case (asin, values) => {
          val average = values.sum * 1.0 / values.size
          (asin,average)
        }
      }
    )
//    val aggData = reviewRDD.map(row => (row.getString(0),row.getDouble(2))).groupByKey().mapValues(_.sum)
    println("The number of data is: " + aggData.count())
    spark.close()
  }
}
