package org.ucf.spark.ScalaWordCount
import java.io.File
import java.io.IOException
import java.nio.charset.Charset
import org.apache.commons.io.FileUtils
object LocalFileService {
  val encoding = Charset.forName("utf-8")
  
  /** read a file from local and save as a list of string*/
  def readLines(path:String) = FileUtils.readLines(new File(path))
  /** Check file whether or not exist*/
  def isExists(path:String) = (new File(path)).exists()
  /** Writes a String to a file creating the file if it does not exist.*/
  def write(path:String,data:String,append:Boolean) = 
      FileUtils.writeStringToFile(new File(path), data, encoding, append)
      
      
  def removeFile(path:String) = (new File(path)).delete()  
}