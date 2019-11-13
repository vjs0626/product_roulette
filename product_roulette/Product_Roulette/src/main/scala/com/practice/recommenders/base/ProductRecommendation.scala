package com.practice.recommenders.base
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import com.practice.recommenders.util._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.util.Random
import org.apache.spark.sql.SparkSession

class ProductRecommendation(spark: SparkSession) {
  
  import spark.implicits._
  val sc = spark.sparkContext
  val configDetails = new ConfigConstants
  val dataUtil = new DataUtil(spark)
  
  def finalStageRecommendations(userID: Int, category: String) : Seq[String] = {
    
    var productList: Seq[String] = null
    
    if(dataUtil.checkNewUser(userID,category))
    {
      val recommendationResults = sc.textFile(configDetails.dataBasePath + "/user_recommendation_data/"+category+"/*")
    
      val userRecommendations = recommendationResults.map(_.split(",")).filter(x => x(0).equals(userID)).map(x => x(1)).collect()
    
      userRecommendations.foreach{ x=> 
        productList = productList :+ x
      }
    }
    else
    {
      val recommendationResults = sc.textFile(configDetails.dataBasePath + "/category_recommendation_data/"+category+"/*")
      val userRecommendations = recommendationResults.map(_.split(","))
                                //.filter(x => x(0).equals(category))
                                .map(x => x(0)).collect()
      
      userRecommendations.foreach{ x=> 
        productList = productList :+ x
      }
    }
   
    return productList
  }
  
  /*
   * 
   */
  def computeOverallRecommendations(userID: Int,category: String) : Seq[String] = {
    
    var finalRecommendationList : Seq[String] = null
    val stage = computeStage(category)
    if(stage == 1)
      finalRecommendationList = initialRecommendations(category)
    else if(stage == 2)
      finalRecommendationList = midStageRecommendations(category)
    else if(stage == 3)
      finalRecommendationList = finalStageRecommendations(userID, category)
    
    finalRecommendationList
  }
  
  def initialRecommendations(category: String) : Seq[String] = {
    
    val newProductsInfo = sc.textFile(configDetails.dataBasePath + "/new_products/"+category+"/*")
                          .map(_.split(",")(1))
    
    newProductsInfo.takeSample(false, configDetails.numberOFRecommendations).toSeq                      
  }
  
  def midStageRecommendations(category: String) : Seq[String] = {
    var productList: Seq[String] = null
    
    val recommendationResults = sc.textFile(configDetails.dataBasePath + "/category_recommendation_data/"+category+"/*")
    val numberOfRecommendations = configDetails.numberOFRecommendations
    val userRecommendations = recommendationResults.map(_.split(","))
                                .map(x => (x(0),x(1).toDouble))
                                .sortBy(_._2, false).take(numberOfRecommendations - configDetails.newProductsToBeRecommended)
                                .map(x => x._1)
      
      userRecommendations.foreach{ x=> 
        productList = productList :+ x
      }
    
    val newReco = new NewProductsComputation(spark)
    productList ++ newReco.totalNewRecommendations(category)
    
  }
  
  
  def computeCategoryRecommendations(category: String) : Unit = {
    
    val ratings = sc.textFile(configDetails.dataBasePath+"/ratings/"+category+"/*")
    val newProductRatings = sc.textFile(configDetails.dataBasePath+"/new_product_ratings/"+category+"/*")
    val combinedRatings = ratings.union(newProductRatings)
    val productRatings = combinedRatings.map(_.split(",")).map(x => (x(1),x(2).toDouble))
    val numberOfRecommendations = configDetails.numberOFRecommendations
    val zeroes = (0.0,0)
    val meanRatings = productRatings.aggregateByKey(zeroes)(
                            (x,y) => (x._1+y,x._2+1), 
                            (x,y) => (x._1+y._1,x._2+y._2))
                            .mapValues(x => x._1/x._2)                       
    val topProducts = meanRatings.sortBy(_._2, false).take(numberOfRecommendations).toList.toDF()
    val dataIO = new DataIO(spark)
    dataIO.pushData(topProducts, configDetails.dataBasePath+"/category_recommendation_data/"+category+"/", 1, false)
  }
  
  def computeStage(category: String) : Int = {
    
    var stage : Int = 0
    
    val ratings = sc.textFile(configDetails.dataBasePath+"/ratings/"+category+"/*")
    val newProductRatings = sc.textFile(configDetails.dataBasePath+"/new_product_ratings/"+category+"/*")
    val combinedRatings = ratings.union(newProductRatings).count()
    val initialStageThreshold = configDetails.stage1Threshold
    val middleStageThreshold = configDetails.stage2Threshold
    
    if(combinedRatings < initialStageThreshold)
      stage = 1
    else if(combinedRatings > initialStageThreshold && initialStageThreshold<middleStageThreshold)
      stage = 2
    else
      stage = 3

    return stage    
  }
  
  
}