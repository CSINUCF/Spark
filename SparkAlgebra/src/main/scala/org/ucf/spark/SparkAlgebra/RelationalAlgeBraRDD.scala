package org.ucf.spark.SparkAlgebra
import org.apache.spark.{SparkContext,SparkConf}

import org.apache.spark.rdd.RDD
import java.sql.Timestamp



/**
org.ucf.spark.SparkAlgebra.RelationalAlgeBraRDD

* 
* */


object RelationalAlgeBraRDD {
  
  
  def wcmain(args: Array[String]){
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)
    val reg1 = sc.textFile("hdfs:///in/wc.txt",5)
                .flatMap( line => line.split(" ") )
                .map( word => (word,1))
                .groupByKey()
                .map{ case (key,counts) => (key,counts.sum)}
    reg1.collect().foreach( println _)
    
    
    val reg2 = sc.textFile("hdfs:///in/wc.txt",5)
                .flatMap( line => line.split(" ") )
                .map( word => (word,1))
                .reduceByKey(_+_)
                
    reg2.collect().foreach( println _)
  }
  
  
  def nestedRddmain(args: Array[String]){
    val conf = new SparkConf().setAppName("Nested RDD")
    val sc = new SparkContext(conf)
    val rdd1:RDD[Int] = sc.parallelize(List.range(0,100).toSeq,5)
    val rdd2:RDD[Int] = sc.parallelize(List.range(50,100).toSeq,5)
    val reg:RDD[RDD[Int]] = rdd1.map { x => rdd2.map { y => x+ y} } 
    println(reg.collect().length)    
  }
  
  def studentmain(args: Array[String]){
    val conf = new SparkConf().setAppName("Nested RDD")
    val sc = new SparkContext(conf)
    def safeStringToLong(str: String): Long = {
        import scala.util.control.Exception._
        val res = catching(classOf[NumberFormatException]) opt str.toLong
        if(res.isEmpty) 0 else res.head
    }
    /*(LN,Name)*/
    val studentRdd = sc.textFile("/data/rdd/student.txt").map { 
      line => {
        val e = line.split("\t")
        if(e.length < 2){
          (0L,"empy name")
        }else{
         (safeStringToLong(e(0)),e(1))
        }
       }
    }
    /*(BN,content)*/
    val bookRdd = sc.textFile("/data/rdd/book.txt").map { 
      line => {
        val e = line.split("\t")
        if(e.length < 2){
          (0L,"There is not content")
        }else{
          (safeStringToLong(e(0)),e(1))
        }
      }
    }
    /*(LN,BN,date)*/
    val loadRdd = sc.textFile("/data/rdd/load.txt").map { 
      line => {
        val e = line.split("\t")
        if(e.length == 3){
          val ln = safeStringToLong(e(0))
          val bn = safeStringToLong(e(1))
          val date = Timestamp.valueOf(e(2))
          (ln,bn,date)
        }else{
          (0L,0L,Timestamp.valueOf("2015-01-01 00:00:00"))
        }
      }
    }

//val studentRdd = sc.textFile("/data/rdd/student.txt").map { ???}
//val bookRdd = sc.textFile("/data/rdd/book.txt").map { ???}
//val loadRdd = sc.textFile("/data/rdd/load.txt").map { ???}

    val ts = Timestamp.valueOf("2013-05-30 00:00:00.000")
    def f1() = {
      val res = studentRdd.cartesian(loadRdd).cartesian(bookRdd).filter({
        case ((s,l),b) => {
          val lLn = l._1
          val lBn = l._2
          val lDate = l._3
          val sLn = s._1
          val bBn = b._1
           lDate.before(ts)&&(lLn == sLn)&&(lBn == bBn)
        }
      }).map{
        case ((s,l),b) => {
          val sName = s._2
          val bTitle = b._2
          (sName,bTitle)
        }
      }
      res
    }
    def f2() = {
      val lb = loadRdd.filter({
        case (ln,bn,date) => date.before(ts)
      }).map({ case (ln,bn,date) => (ln,bn)})
     val stdLeftOutJoin = studentRdd.leftOuterJoin(lb).filter({
        case (ln,(name,some)) => !some.isEmpty
      }).map{ case (ln,(name,bn)) => (bn.head,name)}
      val res = stdLeftOutJoin.leftOuterJoin(bookRdd).filter({
        case (bn,(name,some)) => !some.isEmpty
      }).map{ case (bn,(name,title)) => (name,title.head)}
      res
    }
    println(f2.collect().foreach(println _))
    sc.stop()
  }
  
  def main(args: Array[String]){
    //wcmain(args)
    studentmain(args)
  }

}