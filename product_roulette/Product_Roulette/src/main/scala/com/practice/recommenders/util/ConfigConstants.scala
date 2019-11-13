package com.practice.recommenders.util

class ConfigConstants {
  
  val dataBasePath = "/Users/vi356913/Documents/personal/product_roulette"
  val rankList = Seq(5,10,15,20)
  val regParamList = Seq(0.001,0.003,0.009,0.01,0.03,0.09,0.1)
  val categoryList = Seq("HR","Developer","Marketing","Sales")
  val trainValidateSplit = Array(0.7.toDouble,0.3.toDouble)
  val numberOfIterations = 20
  val modelPath = dataBasePath + "/model"
  val numberOFRecommendations = 5
  val newProductThresholdRatings = 2
  val newProductsToBeRecommended = 2
  val stage1Threshold = 1
  val stage2Threshold = 2
  
}