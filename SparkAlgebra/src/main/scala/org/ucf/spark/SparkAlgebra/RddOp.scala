package org.ucf.spark.SparkAlgebra

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext._

object RddOp {
	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
		val srcRdd:RDD[String] = sc.textFile("hdfs://...", 5)
	}
}