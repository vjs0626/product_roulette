package com.practice.recommenders.base

import com.practice.recommenders.util.DataUtil
import org.apache.spark.sql.SparkSession
import com.practice.recommenders.util.DataIO
import com.practice.recommenders.util.ConfigConstants
import scala.collection.mutable.Map

class FeedbackHandle(spark: SparkSession) {
  
  import spark.implicits._
  val dataUtil = new DataUtil(spark)
  val dataIO = new DataIO(spark)
  val configDetails = new ConfigConstants
  
  
  def lastRecommemdedList(userID: Int, category: String) : Seq[String] = {
    var prevRecommendationsList: Seq[String] = null
    
    val previousRecoData = spark.sparkContext.textFile(configDetails.dataBasePath + "/previous_user_recommendations/"+category+"/*").map(_.split(","))
    val userRecos = previousRecoData.filter(x => x(0).toInt.equals(userID)).map(x=> x(1)).collect.head
    
    userRecos.split(":").foreach(x => 
      prevRecommendationsList = prevRecommendationsList :+ x)
      
    return prevRecommendationsList
  }
  
  def pushRecommendationForFeedback(userID: Int, category: String, productList: Seq[String]) : Unit = {
    
    var productSeq: StringBuilder = new StringBuilder
    for(counter <- 0 to productList.length - 1)
    {
      if(counter < productList.length - 1)
        productSeq.append(productList(counter) + ":")
      else
        productSeq.append(productList(counter))
    }
    val recommendationList = List(List(userID,productSeq)).toDF()
    
    dataIO.pushData(recommendationList, configDetails.dataBasePath+"/previous_user_recommendations/"+category+"/", 1, true)
  }
  
  def collectFeedback(userID: Int, category: String) : Unit = {
    println("Would like to help us improve?? :(Y/N)")
    val response = scala.io.StdIn.readLine()
    if(response == "y" || response == "Y")
    {
      println("Please choose 3 product names from the list and rate on scale of 5:")
      new FeedbackHandle(spark).lastRecommemdedList(userID, category).foreach(println)
      
      val ratingMap :Map[String,Double] = null
      
      for(counter  <- 1 to 3)
      {
        println("Product Name : ")
        val productName = scala.io.StdIn.readLine()
        println("Rating : ")
        val rating = scala.io.StdIn.readLine().toDouble
        ratingMap.put(productName,rating)
      }
      pushUserRatings(userID, category, ratingMap)
      
    }
  }
  
  def pushUserRatings(userID: Int ,category: String, ratings: Map[String,Double]) : Unit = {
    
    var newRatingsList : List[(Int,Int,Double)] = null
    var existingRatingList : List[(Int,Int,Double)] = null
    ratings.keys.foreach(
      x => {
        if(dataUtil.checkProductInNewList(x,category)){
          newRatingsList = (userID,dataUtil.getProductID(x, category),ratings.getOrElse(x, 0.0)) :: newRatingsList
        }
        else
        {
          existingRatingList = (userID,dataUtil.getProductID(x, category),ratings.getOrElse(x, 0.0)) :: existingRatingList
        }
      }
    )
    
    dataIO.pushData(newRatingsList.toDF(), configDetails.dataBasePath + "/new_product_ratings/"+category+"/", 1, true)
    dataIO.pushData(existingRatingList.toDF(), configDetails.dataBasePath + "/ratings/"+category+"/", 1, true)
  }
  
}