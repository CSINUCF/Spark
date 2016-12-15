package org.ucf.spark.SparkCase
import org.apache.spark.sql.{Column,SparkSession}
import org.ucf.spark.Utility.DBDataLoader
object DataSetAPI {
	def twoTableJoinOp(args:Array[String],spark:SparkSession) = {
	   // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    spark.conf.set("spark.sql.crossJoin.enabled", true)
	  val msgsql = "Created > '2013-05-21 00:00:00' and Created < '2013-06-21 00:00:00'"
	  val linksql = "CreatedDate > '2013-05-21 00:00:00' and CreatedDate < '2013-06-16 00:00:00'"
	  val message = DBDataLoader.loadDataFromMySQL("message",spark,msgsql).select("MsgID","UserID").repartition(5)
	  val link = DBDataLoader.loadDataFromMySQL("Link", spark, linksql).select("LinkID","User1","User2","MsgID").repartition(5)
	  
	  println("message:"+message.count())
	  //message.take(100).foreach { x => println(x.toString()) }
	  println("Link:"+link.count())
	  //link.take(100).foreach { x => println(x.toString()) }
	  
	  
	  
	  datasetAPI("cartisan")
	  
	 
	  def datasetAPI(jType:String) = {
      val msgUser = message.select("UserID")
	    val linkUser = link.select("User1")
      val joinTable = jType match {
        case "cartisan" =>  msgUser.join(linkUser).filter { row => row.getInt(0) == row.getInt(1) }
        case "join" => msgUser.join(linkUser,$"UserID" === $"User1").filter { row => row.getInt(0) == row.getInt(1) }
        case _ => null
      }
      if (joinTable != null){
	      val comt = joinTable
	      println("joinTable"+":"+comt.count())
	      comt.take(100).foreach { x => println(x.toString()) }
      }
	  }
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	  
	}
}