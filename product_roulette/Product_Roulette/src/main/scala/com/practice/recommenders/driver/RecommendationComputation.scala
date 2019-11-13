package com.practice.recommenders.driver

import com.practice.recommenders.base.MatrixfactorizationComputation
import com.practice.recommenders.util.ConfigConstants
import org.apache.spark.sql.SparkSession
import com.practice.recommenders.base.MatrixfactorizationComputation
import org.apache.spark.SparkConf
import com.practice.recommenders.base.ProductRecommendation

object RecommendationComputation {
  
  val configDetails = new ConfigConstants
  def main(args: Array[String]) : Unit = {
    
    val computationType = args(0)
    val category = args(1)
    
     val conf:SparkConf = new SparkConf()
                            .setAppName("RecoComputation")
                            .setMaster("local")
                            .set("spark.sql.warehouse.dir",configDetails.dataBasePath)
    
    val spark = SparkSession
                .builder()
                .config(conf)
                //.enableHiveSupport()
                .getOrCreate()
    if(computationType.equals("MF"))
    {
      val mfCompute = new MatrixfactorizationComputation(spark)
      mfCompute.prepareModel(spark.sparkContext, category)
      mfCompute.computeRecommendations(spark.sparkContext, category)
    }
    else if(computationType.equals("categorical"))
    {
      val categoricalCompute = new ProductRecommendation(spark)
      categoricalCompute.computeCategoryRecommendations(category)
    }
    else {
      println("Wrong computation type entered, Exiting")
      System.exit(0)
    }
  }
}