package com.practice.recommenders.util

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame
import scala.reflect.io.Directory
import java.io.File

class DataIO(spark : SparkSession) {
  
  import spark.implicits._
  
  def readData(filepath:String) = 
  {
      
  }
  def pushData(dataset: DataFrame, filePath: String, numOfFiles: Int, append: Boolean) : Unit = 
  {
    if(append)
    {
      dataset.coalesce(numOfFiles).write.format("csv").mode(SaveMode.Append).save(filePath)
    }
    else
    {
      dataset.coalesce(numOfFiles).write.format("csv").mode(SaveMode.Overwrite).save(filePath)
    }
  }
  /*
   * to be completed
   */
  def renameFolder(currentPath: String, nextPath: String) : Unit = {
    removeFolder(nextPath)
    val directory = new Directory(new File(currentPath))
    
  }
  
  private def removeFolder(path: String) : Unit = {
    val directory = new Directory(new File(path))
    directory.deleteRecursively()
  }
}