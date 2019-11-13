package com.practice.recommenders.base

import com.practice.recommenders.util.ConfigConstants
import org.apache.spark.sql.SparkSession
import com.practice.recommenders.util.DataUtil
import com.practice.recommenders.util.DataIO
import scala.util.Random

class NewProductsComputation(spark: SparkSession) {
  
  val configDetails = new ConfigConstants
  val sc = spark.sparkContext
  val dataUtil = new DataUtil(spark)
  val dataIO = new DataIO(spark)
  import spark.implicits._
  
  /*
   * for moving products from new to main based on 
   * number of ratings
   * a. ratings to be moved
   * b. products to be moved
   */
  def productMovement(category: String) : Unit = {
    
    val productMovementList = computeMovementList(category)
    
    val newProductsInfo = sc.textFile(configDetails.dataBasePath + "/new_products/"+category+"/*")
                          .map(_.split(","))
                          .filter(x => productMovementList.contains(x(0)))
                          .toDF()
    val newProductRatings = sc.textFile(configDetails.dataBasePath+"/new_product_ratings/"+category+"/*")
                          .map(_.split(","))
                          .filter(x => productMovementList.contains(x(1)))
                          .toDF()
    
    
    
    dataIO.pushData(newProductsInfo, configDetails.dataBasePath+"/products/"+category+"/", 1, true)
    dataIO.pushData(newProductRatings, configDetails.dataBasePath+"/ratings/"+category+"/", 1, true)
    
    removeMovedFromNew(productMovementList,category)
  }
  /*
   * Implemented with filter for small datasets
   * to be changed to join for larger ones
   */
  private def topNonRatedNew(category: String) : Seq[String] = {
    
    val newProducts = sc.textFile(configDetails.dataBasePath + "/new_products/"+category+"/*").map(_.split(",")(1)).map(x => (x,0))
    val shownNewProducts = sc.textFile(configDetails.dataBasePath + "/new_product_shown_list/"+category+"/*")
    val newRatedProducts = sc.textFile(configDetails.dataBasePath+"/new_product_ratings/"+category+"/*").map(_.split(",")(1).toInt).distinct()
    
    val ratedProductNames = newRatedProducts.map(x => dataUtil.getProductListByID(Seq(x), category)(0)).collect().toSeq
    val shownProductWithCounts = shownNewProducts.map(x => (x,1)).reduceByKey((x,y) => x+y)
    
    val shownNonRatedProducts = shownProductWithCounts.filter(x => !ratedProductNames.contains(x._1))
    
    val combinedNonrated = shownNonRatedProducts.union(newProducts)
                          .distinct().sortBy(_._2, true).map(x => x._1)
                          .take(configDetails.newProductsToBeRecommended*(5/2)).toSeq
                          
    combinedNonrated
  }
  
  private def topRatedNew(category: String) : Seq[String] = {
    val newProductRatings = sc.textFile(configDetails.dataBasePath+"/new_product_ratings/"+category+"/*").map(_.split(",")).map(x => (x(1),x(2).toDouble))
    val zeroes = (0.0,0)
    val meanRatings = newProductRatings.aggregateByKey(zeroes)(
                            (x,y) => (x._1+y,x._2+1), 
                            (x,y) => (x._1+y._1,x._2+y._2))
                            .mapValues(x => x._1/x._2)
                            
    val topProducts = meanRatings.sortBy(_._2, false).map(x => x._1).take(configDetails.newProductsToBeRecommended*(3/2))
    
    topProducts.toSeq
  }
  
  /*
   * include append to shown list at the end
   */
  def totalNewRecommendations(category: String) : Seq[String] = {
    
    val nonRatedProducts = topNonRatedNew(category)
    val ratedProducts = topRatedNew(category)
    
    var finalNonRated : Seq[String] = null
    var finalRated : Seq[String] = null
    
    val numberOfElemFromEach = configDetails.newProductsToBeRecommended/2
    val random = new Random
    for(i <- 1 to numberOfElemFromEach)
    {
      finalNonRated = finalNonRated :+ nonRatedProducts(random.nextInt(nonRatedProducts.length))
      finalRated = finalRated :+ ratedProducts(random.nextInt(ratedProducts.length))
    }
    
    
    val finalProducts:Seq[String] = finalNonRated ++ finalRated
    
    new DataIO(spark).pushData(finalProducts.toList.toDF, configDetails.dataBasePath+"/new_product_shown_list/"+category+"/", 1, true)
    
    
    return finalProducts
    
  }
  
  def computeMovementList(category: String) : Seq[Int] = {
    
    var productMoveList:Seq[Int] = null
    val newProductRatings = sc.textFile(configDetails.dataBasePath+"/new_product_shown_list/"+category+"/*").map(_.split(",")).map(x => (x(1),1))
    
    val productMigrationThreshold = configDetails.newProductThresholdRatings
    productMoveList = newProductRatings.reduceByKey((x,y) => x+y).filter(x => x._2 > productMigrationThreshold).map(x => x._1.toInt).collect().toSeq
    
    return productMoveList
  }
  
  private def removeMovedFromNew(newProductList : Seq[Int],category: String) : Unit = {
    
    val newProductsInfo = sc.textFile(configDetails.dataBasePath + "/new_products/"+category+"/*")
                          .map(_.split(","))
                          .filter(x => !newProductList.contains(x(0)))
                          .toDF()
    val newProductRatings = sc.textFile(configDetails.dataBasePath+"/new_product_ratings/"+category+"/*")
                          .map(_.split(","))
                          .filter(x => !newProductList.contains(x(1)))
                          .toDF()
                          
    dataIO.pushData(newProductsInfo, configDetails.dataBasePath+"/new_products/"+category+"_tmp/", 1, false)
    dataIO.pushData(newProductRatings, configDetails.dataBasePath+"/new_product_ratings/"+category+"_tmp/", 1, false)
    
    dataIO.renameFolder(configDetails.dataBasePath+"/new_product_ratings/"+category+"_tmp", configDetails.dataBasePath+"/new_product_ratings/"+category)
  }
  
}