package org.ucf.spark.Utility

import com.typesafe.config.{Config, ConfigFactory}
import java.io.File
object DBPropertiesLoader {
	private val conf:Config = ConfigFactory.load("application.conf")
//	private val parsedConfig = ConfigFactory.parseFile(new File("/home/bing/command/spark/sentiment/application.conf"))
//  private val conf = ConfigFactory.load(parsedConfig)
	
	val mysqlURL = conf.getString("MYSQL_URL")
	val mysqlUser = conf.getString("MYSQL_USER")
	val mysqlPasswd = conf.getString("MYSQL_PASSWD")
	val mysqlDriver = conf.getString("MYSQL_DRIVER")
}