package org.ucf.spark.SparkCase
import org.apache.spark.sql.SparkSession

object SparkEntrance {
	def main(args: Array[String]) {
	  val spark = SparkSession
                  .builder()
                  .appName("Spark example")
                  .config("spark.some.config.option", "some-value")
                  .getOrCreate()
                  
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._          
    DataSetAPI.twoTableJoinOp(args, spark)              
                  
	}
}