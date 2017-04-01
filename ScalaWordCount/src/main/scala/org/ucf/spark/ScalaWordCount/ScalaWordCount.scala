package org.ucf.spark.ScalaWordCount
import org.apache.spark.sql.SparkSession

/**
 * @author Bing
 * Date 2017-03-20
 * Reference
 * 1. https://databricks.com/blog/2016/01/04/introducing-apache-spark-datasets.html
 * 2. http://blog.madhukaraphatak.com/introduction-to-spark-two-part-2/
 */
object ScalaWordCount{
   def main(args: Array[String]) {
     val startTime = System.currentTimeMillis()
     val inputFile = args(0)
     val option = args(3).toInt
     val path="/home/bing/result/wc/"+"result.txt"
     option match {
       case 1 => { //default
         val maxCores = 48
         val sparkSession = SparkSession.builder()
                             .appName("ScalaWordCount")
                             .config("spark.executor.cores", 2)
                             .config("spark.executor.memory","4g")
                             .config("spark.driver.cores",2)
                             .config("spark.driver.memory","4g")
                             .getOrCreate()
         import sparkSession.implicits._
          
         val data = sparkSession.read.text(inputFile).as[String]
         val numsP = data.rdd.getNumPartitions
         val words = data.flatMap(value => value.split("\\s+"))
         val groupedWords = words.groupByKey(_.toLowerCase)
         val appId = sparkSession.sparkContext.applicationId
         val counts = groupedWords.count().collectAsList()
         val stopTime = System.currentTimeMillis()
         val numsExecutor = sparkSession.sparkContext.getExecutorStorageStatus.size - 1
         val size = counts.size
         val time = stopTime-startTime
         val result = s"${appId}\t${option}\t${maxCores}\t${numsExecutor}\t${numsP}\t${time}\t${size}\n"
         LocalFileService.write(path, result, true)
         sparkSession.stop()     
       }
       case 2 => { // set number
         val maxCores = args(1).toLong
         val numsPartion = args(2).toInt
         val sparkSession = SparkSession.builder()
                             .appName("ScalaWordCount")
                             .config("spark.cores.max",maxCores)
                             .config("spark.executor.cores", 2)
                             .config("spark.executor.memory","4g")
                             .config("spark.driver.cores",2)
                             .config("spark.driver.memory","4g")
                             .getOrCreate()
         import sparkSession.implicits._
          
         val data = sparkSession.read.text(inputFile).repartition(numsPartion).as[String]
         val numsP = data.rdd.getNumPartitions
         val words = data.flatMap(value => value.split("\\s+"))
         val groupedWords = words.groupByKey(_.toLowerCase)
         val appId = sparkSession.sparkContext.applicationId
         val counts = groupedWords.count().collectAsList()
         val stopTime = System.currentTimeMillis()
         val numsExecutor = sparkSession.sparkContext.getExecutorStorageStatus.size - 1
         val size = counts.size
         val time = stopTime-startTime
         val result = s"${appId}\t${option}\t${maxCores}\t${numsExecutor}\t${numsP}\t${time}\t${size}\n"
         LocalFileService.write(path, result, true)
         sparkSession.stop()
       }
       case _ => {
         
       }
     }
   }
  
}