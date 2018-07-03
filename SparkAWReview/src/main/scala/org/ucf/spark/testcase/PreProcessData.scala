package org.ucf.spark.testcase

import org.apache.spark.sql.SparkSession

trait PreProcessData {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val spark = SparkSession.builder()
      .appName("AmazonReviews_PreProcess")
      .getOrCreate()
    import spark.implicits._
    /*
        scala> product.printSchema
        root
        |-- _corrupt_record: string (nullable = true)
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
    val productMeta = spark.read.textFile("/data/amazon/ProductMeta_src_6.6G.txt")
    val pPreg = productMeta.filter(row => !(row.equals("[") || row.equals("]"))).map(row => row.drop(2).dropRight(1))

    pPreg.write.text("/data/amazon/ProductMeta_na_6.6G.jsonl")

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
    val review = spark.read.textFile("/data/amazon/Reviews_src_89G.txt")
    val pRreg = review.filter(row => !(row.equals("[") || row.equals("]"))).map(row => row.drop(2).dropRight(1))

    pRreg.write.text("/data/amazon/Reviews_na_89G.jsonl")

    pRreg.sample(0.5).write.text("/data/amazon/Reviews_na_45G.jsonl")

    pRreg.sample(0.25).write.text("/data/amazon/Reviews_na_23G.jsonl")

    spark.close()
    val stopTime = System.currentTimeMillis()
    val time = stopTime-startTime
    println("The total execution time is: " + time)
  }
}



trait PreProcessData_NA {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val spark = SparkSession.builder()
      .appName("AmazonReviews_PreProcess_NA")
      .getOrCreate()
    /*
        scala> product.printSchema
        root
        |-- _corrupt_record: string (nullable = true)
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
    //val productMeta = spark.read.json("/data/amazon/ProductMeta_na_6.6G.jsonl")
    //val pPreg = productMeta.drop("_corrupt_record").na.drop()

    //pPreg.write.mode("append").json("/data/amazon/ProductMeta_6.6G.jsonl")

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
    val review = spark.read.json("/data/amazon/Reviews_na_89G.jsonl")
    val pRreg = review.drop("_corrupt_record").na.drop()

    pRreg.write.mode("append").json("/data/amazon/Reviews_large.jsonl")

    pRreg.sample(0.5).write.mode("append").json("/data/amazon/Reviews_medium.jsonl")

    pRreg.sample(0.25).write.mode("append").json("/data/amazon/Reviews_small.jsonl")

    spark.close()
    val stopTime = System.currentTimeMillis()
    val time = stopTime-startTime
    println("The total execution time is: " + time)
  }
}
