package com.practice.recommenders.util

import org.apache.spark.sql.SparkSession

class DataUtil(spark: SparkSession) {
  
  val configDetails = new ConfigConstants
  val sc = spark.sparkContext
  
  def generateUserID(user_email: String,category: String) : Int = 
  {
    val userData = sc.textFile(configDetails.dataBasePath+"/users/"+category+"/*")
    var maxUserID = 0
    try
    {
      maxUserID = userData.map(_.split(",")(0).toInt).max()
    }
    catch
    {
      case e: Exception => maxUserID = 0
                           
    }
    return maxUserID + 1
  }
  
  def generateProductID(product_name: String,category: String) : Int = 
  {
    var maxProductID = 0
    val newCategoryProducts = sc.textFile(configDetails.dataBasePath + "/new_products/"+category+"/*")
    val existingCategoryProducts = sc.textFile(configDetails.dataBasePath + "/products/"+category+"/*")
    try
    {
    maxProductID = existingCategoryProducts.union(newCategoryProducts).map(_.split(",")(0).toInt).max()
    }
    catch
    {
      case e: Exception => maxProductID = 0
                           
    }
    return maxProductID + 1
  }
  
  def getUserID(userMailID: String, category: String) : Int = 
  {
    val userData = sc.textFile(configDetails.dataBasePath+"/users/"+category+"/*").map(_.split(","))
    userData.filter(x => x(1).equals(userMailID)).collect().head(0).toInt
    
  }
  
  def getProductID(product_name: String,category: String) : Int = {
    
    val newCategoryProducts = sc.textFile(configDetails.dataBasePath + "/new_products/"+category+"/*")
    val existingCategoryProducts = sc.textFile(configDetails.dataBasePath + "/products/"+category+"/*")
    existingCategoryProducts.union(newCategoryProducts).map(_.split(",")).filter(x => x(1).equals(product_name)).collect().head(0).toInt
  }
  
  def computeMeanProductRating(productID: Int) : Double = 
  {
    return 0.0
  }
  
  def checkExistingUser(userMailID: String, category: String) : Boolean = 
  {
    val userData = sc.textFile(configDetails.dataBasePath+"/users/"+category+"/*").map(_.split(","))
    return userData.filter(x => x(1).equals(userMailID)).count() > 0
  }
  
  def checkNewUser(userID:Int, category: String) : Boolean = 
  {
    val ratings = sc.textFile(configDetails.dataBasePath+"/ratings/"+category+"/*").map(_.split(","))
    val newProductRatings = sc.textFile(configDetails.dataBasePath+"/new_product_ratings/"+category+"/*").map(_.split(","))
    
    return ratings.union(newProductRatings).filter(x => x(0).toInt.equals(userID)).count() > 0
  }
  
  def checkExistingProduct(productName: String, category: String) : Boolean = 
  {
    
    val newCategoryProducts = sc.textFile(configDetails.dataBasePath + "/new_products/"+category+"/*")
    val existingCategoryProducts = sc.textFile(configDetails.dataBasePath + "/products/"+category+"/*")
    return  existingCategoryProducts.union(newCategoryProducts).map(_.split(",")).filter(x => x(1).equals(productName)).count() > 0
    
  }
  
  def getProductListByID(productList: Seq[Int], category: String) : Seq[String] = {
    
    var productSeq: Seq[String] = null
    val newCategoryProducts = sc.textFile(configDetails.dataBasePath + "/new_products/"+category+"/*")
    val existingCategoryProducts = sc.textFile(configDetails.dataBasePath + "/products/"+category+"/*")
    val productArray = existingCategoryProducts.union(newCategoryProducts).map(_.split(",")).filter(x => productList.contains(x(0))).map(x => x(1)).collect
    
    productArray.foreach({
      x => productSeq :+ x
    })
    
    productSeq
  }
  
  def checkProductInNewList(productName: String,category: String) : Boolean = {
    
    val newCategoryProducts = sc.textFile(configDetails.dataBasePath + "/new_products/"+category+"/*")
    return newCategoryProducts.map(_.split(",")).filter(x => x(1).equals(productName)).count() > 0
    
  }
  
  def registerNewUser(spark: SparkSession,userID: Int, userMailID: String,category: String) : Unit = {
    
    import spark.implicits._
    val userDF = List(List(userID,userMailID,category)).map(x => (x(0),x(1))).toDF
    val dataIO = new DataIO(spark)
    dataIO.pushData(userDF, configDetails.dataBasePath + "/data/users/"+category+"/", 1, true)
    
  }
}