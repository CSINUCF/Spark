package org.ucf.spark.testcase

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.ucf.spark.reviewData

trait FilterMove {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AmazonReviews_FM")
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
    val words = reviewRDD.map(row => {
      val asin = row.getString(0)
      val overall = row.getDouble(2)
      val reviewText = row.getString(3)
      val count = reviewText.split(" ").length
      (asin, overall, count)
    })
    val getOverall = words.filter({
      case (asin, overall, count) => overall > 4
    }).map(ele => (ele._1,ele._3))
    val aggData = getOverall.groupByKey().map({
      case (asin, nums) => {
        val average = nums.sum * 1.0 / nums.size
        (asin, average)
      }
    })
    println("The number of data is: " + aggData.count())
    spark.close()
  }
}

trait FilterMoveOptimzied {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AmazonReviews_FM_Optimzied")
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
    val aggData = words.groupByKey().map({
      case (asin, nums) => {
        val average = nums.sum * 1.0 / nums.size
        (asin, average)
      }
    })
    println("The number of data is: " + aggData.count())
    spark.close()
  }
}