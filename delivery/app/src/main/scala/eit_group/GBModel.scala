package eit_group

import SimpleModelObject._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, CrossValidatorModel}


object GBModelObject extends App {
  class GBModel(name: String) extends SimpleModel(name) {
    override def train(training: DataFrame): PipelineModel = {

      val monthIndexer = new StringIndexer().setInputCol("Month").setOutputCol("MonthIndex")
      val dayIndexer = new StringIndexer().setInputCol("DayOfWeek").setOutputCol("DayOfWeekIndex")

      val encoder = new OneHotEncoderEstimator()
        .setInputCols(Array(monthIndexer.getOutputCol, dayIndexer.getOutputCol))
        .setOutputCols(Array("MonthVec", "DayOfWeekVec"))

      val assembler = new VectorAssembler()
        .setInputCols(Array("MonthVec","DayOfWeekVec","DepDelay","Distance","TaxiOut","Hour","NightFlight"))
        .setOutputCol("features")
//      val output = assembler.transform(training)
//      output.show(truncate=false)

      val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexFeatures")
        .setMaxCategories(12)

      val gbt = new GBTRegressor()
        .setLabelCol("ArrDelay")
        .setFeaturesCol("indexFeatures")
//        .setMaxIter(100)
        .setSubsamplingRate(0.8)
        .setLossType("squared")

      val evaluator = new RegressionEvaluator()
        .setMetricName("rmse")
        .setLabelCol("ArrDelay")
        .setPredictionCol("prediction")
      val paramGrid = new ParamGridBuilder()
        .addGrid(gbt.maxDepth, Array(2,5,7))
        .addGrid(gbt.maxIter, Array(10,20,30))
        .build()
      val cv = new CrossValidator()
        .setEstimator(gbt)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(paramGrid)
        .setNumFolds(3)  // Use 3+ in practice

      // Chain indexer and GBT in a Pipeline.
      val pipeline = new Pipeline()
        .setStages(Array(monthIndexer, dayIndexer, encoder, assembler,featureIndexer, cv))

      // Train model. This also runs the indexer.
      val model = pipeline.fit(training)

//      val gbtModel = model.stages(2).asInstanceOf[GBTRegressionModel]
//      println("Learned regression GBT model:\n" + gbtModel.toDebugString)

      model
    }
  }
}