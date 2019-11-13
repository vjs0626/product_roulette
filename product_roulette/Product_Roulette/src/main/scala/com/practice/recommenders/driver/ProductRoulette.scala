package com.practice.recommenders.driver

import com.practice.recommenders.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.practice.recommenders.util.DataUtil
import com.practice.recommenders.base.ProductRecommendation
import scala.collection.mutable.Map
import com.practice.recommenders.base.FeedbackHandle

object ProductRoulette {
  
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
    
    val dataUtil = new DataUtil(spark)
    println("Please enter mail-id and job category!")
    println("Mail-ID : ")
    val userMailID = scala.io.StdIn.readLine()
    println("Job Category : ")
    val category = scala.io.StdIn.readLine()
    
    var userID : Int = 0
    
    if(dataUtil.checkExistingUser(userMailID,category))
    {
      userID = dataUtil.getUserID(userMailID, category)
      new FeedbackHandle(spark).collectFeedback(userID,category)
    }
    else
    {
      userID = dataUtil.generateUserID(userMailID, category)
      dataUtil.registerNewUser(spark,userID,userMailID,category)
    }
    
    val recommendationList = new ProductRecommendation(spark).computeOverallRecommendations(userID, category)
    
    println("!!!!!!!!!----- Recommendations ------!!!!!!!!")
    for(reco <- recommendationList)
      println(reco)
    
    new FeedbackHandle(spark).pushRecommendationForFeedback(userID,category,recommendationList)
  }
  
}