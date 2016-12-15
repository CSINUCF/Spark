package org.ucf.spark.Utility

import java.util.Properties
import org.apache.spark.sql.{DataFrame,Row,Column,SparkSession}


object DBDataLoader {
  private val url = DBPropertiesLoader.mysqlURL
  private val mySQLprop = new Properties()
  mySQLprop.setProperty("user",DBPropertiesLoader.mysqlUser)
  mySQLprop.setProperty("password",DBPropertiesLoader.mysqlPasswd)
  mySQLprop.setProperty("driver",DBPropertiesLoader.mysqlDriver)
  
	/**
	 * load all data from MySQL database
	 */
	def loadDataFromMySQL(table:String,
	    spark:SparkSession):DataFrame = spark.read.jdbc(url,table,mySQLprop)
	
	/**
	 * Load the data which satisfy the condition sql from MySQL database
	 */
	def loadDataFromMySQL(table:String,
	                      spark:SparkSession,
	                      sql:String):DataFrame = 
	  spark.read.jdbc(url,table,Array(sql),mySQLprop)

	def loadDataFromText(path:String,
	    spark:SparkSession):DataFrame = spark.read.text(path)
}