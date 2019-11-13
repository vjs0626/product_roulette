package com.practice.recommenders.base
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import com.practice.recommenders.util._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType


class MatrixfactorizationComputation(spark: SparkSession) {
  
  val configDetails = new ConfigConstants
  val dataIO = new DataIO(spark)
  import spark.implicits._
  /*
   * Returns root means squared error for given model over the validation set
   */
  private def computeRMSE(validationSet: RDD[Rating],model: MatrixFactorizationModel) :Double = {
    
    val userProductList = validationSet.map{
      case Rating(user,product,rating) => (user,product)
    }
    
    val predictions = model.predict(userProductList).map{
      case Rating(user,product,rating) => ((user,product),rating)
    }
    
    val ratingAndPredictions = validationSet.map{
      case Rating(user,product,rating) =>  ((user,product),rating)
    }.join(predictions)
    
    val mse = ratingAndPredictions.map {
      case((user,product),(r1,r2)) => 
        val error = r1-r2
        error*error
    }.mean()
    
    return Math.sqrt(mse)
  }
  
  
  /*
   * returns the best model based on rmse for 
   * different combinations of latent dimensions
   * and regularization paramter 
   */
  private def getBestModel(trainingSet: RDD[Rating],validationSet: RDD[Rating], maxIterations: Int,
      ranks: Seq[Int], regParams: Seq[Double]) : MatrixFactorizationModel = 
  {
    
    var currentModel : MatrixFactorizationModel = null
    var currentError = Double.MaxValue
    
    for(rank <- ranks)
    {
      for(regParam <- regParams)
      {
        val model = ALS.train(trainingSet, rank, maxIterations, regParam)
        val error = computeRMSE(validationSet, currentModel)
        if(error < currentError)
        {
          currentError = error
          currentModel = model
        }
       }
    }
    
    return currentModel
  }
  
  
  
  /*
   * computes and saves the best model for given category
   */
  def prepareModel(sc : SparkContext, category : String) : Unit = 
  {
    
    val userRatingPath : String = configDetails.dataBasePath + "/" + "ratings/" + category + "/*."
    val overallRatingsSet = sc.textFile(userRatingPath)
    
    val ratings = overallRatingsSet.map(_.split(",") match {
      case Array(user,product,rating) => Rating(user.toInt,product.toInt,rating.toDouble) 
    })
    
    val ratingSplitSet = ratings.randomSplit(configDetails.trainValidateSplit, 0)
    var trainingSet : RDD[Rating] = null
    var validationSet : RDD[Rating] = null
    
    if(ratingSplitSet!=null && ratingSplitSet.length == 2)
    {
      trainingSet = ratingSplitSet(0)
      validationSet = ratingSplitSet(1)
    }
    
    val maxIterations = configDetails.numberOfIterations
    val rankSet = configDetails.rankList
    val regParams = configDetails.regParamList
    
    val finalModel = getBestModel(trainingSet, validationSet, maxIterations, rankSet, regParams)
    
    finalModel.save(sc, configDetails.modelPath+"/"+category)
  }
  
  
  /*
   * Computing recommendations for users
   */
  def computeRecommendations(sc : SparkContext, category : String) : Unit = {
   
    val schemaString = "userID product rating"
    val schema = StructType(schemaString.split(" ").map(fieldName ⇒ StructField(fieldName, StringType, true)))

    
    var finalDf : DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
    val userRatingPath : String = configDetails.dataBasePath + "/" + "ratings/" + category + "/*"
    val overallUserSet = sc.textFile(userRatingPath).map(_.split(",")(0).toInt).distinct().collect()
    
    val finalModel = MatrixFactorizationModel.load(sc, configDetails.modelPath+category)
    
    for(user <- overallUserSet)
    {
     
      val recoDf = finalModel.recommendProducts(user, configDetails.numberOFRecommendations - configDetails.newProductsToBeRecommended)
      .map({
        case Rating(user, product, rating) => (user,product,rating)
      }).toList.toDF
      
      finalDf = finalDf.union(recoDf)
    }
    dataIO.pushData(finalDf, configDetails.dataBasePath+"/user_recommendation_data/"+category+"/", 1, false)
    //recommendationSet.map(x => x.user)
    
  }
}