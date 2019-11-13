package com.practice.recommenders.driver

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.practice.recommenders.util._
import org.apache.spark.sql.SparkSession

object NewProductOnboard {
  
  val configDetails = new ConfigConstants
  
  
  def main(args : Array[String]) : Unit = 
  {
    
    val conf:SparkConf = new SparkConf()
                            .setAppName("ProductOnboard")
                            .setMaster("local")
                            .set("spark.sql.warehouse.dir",configDetails.dataBasePath)
    
    val spark = SparkSession
                .builder()
                .config(conf)
                //.enableHiveSupport()
                .getOrCreate()
    
    import spark.implicits._
    
    println("Enter product name, product manufacturer and product category")
    println("Product name : ")
    val productName = scala.io.StdIn.readLine()
    println("Product manufacturer : ")
    val productMnf = scala.io.StdIn.readLine()
    println("Product category : ")
    val category = scala.io.StdIn.readLine()
    
    val dataIO = new DataIO(spark)
    val dataUtil = new DataUtil(spark)
    val productID = dataUtil.generateProductID(productName, category)
    val productDF = List(List(productID,productName,productMnf)).map(x => (x(0),x(2),x(2))).toDF
    dataIO.pushData(productDF, configDetails.dataBasePath + "/data/products/"+category+"/", 1, true)
    
  }
}