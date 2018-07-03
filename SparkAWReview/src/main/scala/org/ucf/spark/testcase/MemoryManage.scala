package org.ucf.spark.testcase

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.ucf.spark.{metaData, reviewData}

trait MemoryManage {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AmazonReviews_MM")
      .getOrCreate()
    /*
        scala> product.printSchema
        root
        |-- asin: string (nullable = true)
        |-- brand: string (nullable = true)
        |-- categories: array (nullable = true)
        |    |-- element: array (containsNull = true)
        |    |    |-- element: string (containsNull = true)
        |-- imUrl: string (nullable = true)
        |-- price: double (nullable = true)
        |-- related: struct (nullable = true)
        |    |-- also_bought: array (nullable = true)
        |    |    |-- element: string (containsNull = true)
        |    |-- also_viewed: array (nullable = true)
        |    |    |-- element: string (containsNull = true)
        |    |-- bought_together: array (nullable = true)
        |    |    |-- element: string (containsNull = true)
        |    |-- buy_after_viewing: array (nullable = true)
        |    |    |-- element: string (containsNull = true)
        |-- salesRank: struct (nullable = true)
        |    |-- Appliances: long (nullable = true)
        |    |-- Arts, Crafts & Sewing: long (nullable = true)
        |    |-- Automotive: long (nullable = true)
        |    |-- Baby: long (nullable = true)
        |    |-- Beauty: long (nullable = true)
        |    |-- Books: long (nullable = true)
        |    |-- Camera &amp; Photo: long (nullable = true)
        |    |-- Cell Phones & Accessories: long (nullable = true)
        |    |-- Clothing: long (nullable = true)
        |    |-- Computers & Accessories: long (nullable = true)
        |    |-- Electronics: long (nullable = true)
        |    |-- Gift Cards Store: long (nullable = true)
        |    |-- Grocery & Gourmet Food: long (nullable = true)
        |    |-- Health & Personal Care: long (nullable = true)
        |    |-- Home &amp; Kitchen: long (nullable = true)
        |    |-- Home Improvement: long (nullable = true)
        |    |-- Industrial & Scientific: long (nullable = true)
        |    |-- Jewelry: long (nullable = true)
        |    |-- Kitchen & Dining: long (nullable = true)
        |    |-- Magazines: long (nullable = true)
        |    |-- Movies & TV: long (nullable = true)
        |    |-- Music: long (nullable = true)
        |    |-- Musical Instruments: long (nullable = true)
        |    |-- Office Products: long (nullable = true)
        |    |-- Patio, Lawn & Garden: long (nullable = true)
        |    |-- Pet Supplies: long (nullable = true)
        |    |-- Prime Pantry: long (nullable = true)
        |    |-- Shoes: long (nullable = true)
        |    |-- Software: long (nullable = true)
        |    |-- Sports &amp; Outdoors: long (nullable = true)
        |    |-- Toys & Games: long (nullable = true)
        |    |-- Video Games: long (nullable = true)
        |    |-- Watches: long (nullable = true)
        |-- title: string (nullable = true)
    */
    val productRDD:RDD[Row] = spark.read.json(metaData).rdd
    val reviewRDD:RDD[Row] = spark.read.json(reviewData).rdd

    val meta = productRDD.map(row => (row.getString(0),row.getString(1)))

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
    val review = reviewRDD.map(row => (row.getString(0),row.getDouble(2)))
    val reg = review.join(meta).map({
      case (asin, (value, brand)) => (brand,value)
    }).groupByKey().map({
      case (brand, values) => {
        val average = values.sum * 1.0 / values.size
        (brand,values)
      }
    })
    println("The number of data is: " + reg.count())
    spark.close()
  }
}

trait MemoryManageOptimized {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("AmazonReviews_MM_Optimized")
      .getOrCreate()
    /*
        scala> product.printSchema
        root
        |-- asin: string (nullable = true)
        |-- brand: string (nullable = true)
        |-- categories: array (nullable = true)
        |    |-- element: array (containsNull = true)
        |    |    |-- element: string (containsNull = true)
        |-- imUrl: string (nullable = true)
        |-- price: double (nullable = true)
        |-- related: struct (nullable = true)
        |    |-- also_bought: array (nullable = true)
        |    |    |-- element: string (containsNull = true)
        |    |-- also_viewed: array (nullable = true)
        |    |    |-- element: string (containsNull = true)
        |    |-- bought_together: array (nullable = true)
        |    |    |-- element: string (containsNull = true)
        |    |-- buy_after_viewing: array (nullable = true)
        |    |    |-- element: string (containsNull = true)
        |-- salesRank: struct (nullable = true)
        |    |-- Appliances: long (nullable = true)
        |    |-- Arts, Crafts & Sewing: long (nullable = true)
        |    |-- Automotive: long (nullable = true)
        |    |-- Baby: long (nullable = true)
        |    |-- Beauty: long (nullable = true)
        |    |-- Books: long (nullable = true)
        |    |-- Camera &amp; Photo: long (nullable = true)
        |    |-- Cell Phones & Accessories: long (nullable = true)
        |    |-- Clothing: long (nullable = true)
        |    |-- Computers & Accessories: long (nullable = true)
        |    |-- Electronics: long (nullable = true)
        |    |-- Gift Cards Store: long (nullable = true)
        |    |-- Grocery & Gourmet Food: long (nullable = true)
        |    |-- Health & Personal Care: long (nullable = true)
        |    |-- Home &amp; Kitchen: long (nullable = true)
        |    |-- Home Improvement: long (nullable = true)
        |    |-- Industrial & Scientific: long (nullable = true)
        |    |-- Jewelry: long (nullable = true)
        |    |-- Kitchen & Dining: long (nullable = true)
        |    |-- Magazines: long (nullable = true)
        |    |-- Movies & TV: long (nullable = true)
        |    |-- Music: long (nullable = true)
        |    |-- Musical Instruments: long (nullable = true)
        |    |-- Office Products: long (nullable = true)
        |    |-- Patio, Lawn & Garden: long (nullable = true)
        |    |-- Pet Supplies: long (nullable = true)
        |    |-- Prime Pantry: long (nullable = true)
        |    |-- Shoes: long (nullable = true)
        |    |-- Software: long (nullable = true)
        |    |-- Sports &amp; Outdoors: long (nullable = true)
        |    |-- Toys & Games: long (nullable = true)
        |    |-- Video Games: long (nullable = true)
        |    |-- Watches: long (nullable = true)
        |-- title: string (nullable = true)
    */
    val productRDD:RDD[Row] = spark.read.json(metaData).rdd.cache()
    val reviewRDD:RDD[Row] = spark.read.json(reviewData).rdd.cache()

    val meta = productRDD.map(row => (row.getString(0),row.getString(1)))

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

    val review = reviewRDD.map(row => (row.getString(0),row.getDouble(2)))
    val reg = review.join(meta).map({
      case (asin, (value, brand)) => (brand,value)
    }).groupByKey().map({
      case (brand, values) => {
        val average = values.sum * 1.0 / values.size
        (brand,values)
      }
    })
    println("The number of data is: " + reg.count())
    spark.close()
  }
}
