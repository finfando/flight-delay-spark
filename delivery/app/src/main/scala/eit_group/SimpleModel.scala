package eit_group

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.StringIndexer

object SimpleModelObject extends App {
  class SimpleModel(name: String) {
    def train(training: DataFrame): PipelineModel = {

      val monthIndexer = new StringIndexer().setInputCol("Month").setOutputCol("MonthIndex")
      val dayIndexer = new StringIndexer().setInputCol("DayOfWeek").setOutputCol("DayOfWeekIndex")
      val companyIndexer = new StringIndexer().setInputCol("UniqueCarrier").setOutputCol("UniqueCarrierIndex")

      val encoder = new OneHotEncoderEstimator()
        .setInputCols(Array(monthIndexer.getOutputCol,dayIndexer.getOutputCol,companyIndexer.getOutputCol))
        .setOutputCols(Array("MonthVec", "DayOfWeekVec","CompanyVec"))

      val assembler = new VectorAssembler()
        .setInputCols(Array("DepDelay","TaxiOut","Distance","MonthVec","NightFlight","CompanyVec"))
        .setOutputCol("features")

//     val normalizer = new Normalizer()
//        .setInputCol("ContFeatures")
//        .setOutputCol("normFeatures")
//        .setP(1.0)

//      val assembler2 = new VectorAssembler()
//        .setInputCols(Array("normFeatures"))
//        .setOutputCol("features")

      val lr = new LinearRegression()
        .setFeaturesCol("features")
        .setLabelCol("ArrDelay")
        .setMaxIter(30)
        .setElasticNetParam(0.5)



      val pipeline = new Pipeline()
        .setStages(Array(monthIndexer,dayIndexer,companyIndexer,encoder,assembler, lr))

      val lrModel = pipeline.fit(training)//.select("ArrDelay","DepDelay","MonthVec","DayOfWeekVec", "NightFlight"))
      println(s"Coefficients: ${lrModel.stages(5).asInstanceOf[LinearRegressionModel].coefficients}")
      println(s"Intercept: ${lrModel.stages(5).asInstanceOf[LinearRegressionModel].intercept}")
      val trainingSummary = lrModel.stages(5).asInstanceOf[LinearRegressionModel].summary
      println(s"numIterations: ${trainingSummary.totalIterations}")
      println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
//      trainingSummary.residuals.show()
      println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
      println(s"r2: ${trainingSummary.r2}")
      lrModel
    }
    def evaluate(test: DataFrame, pipeline: PipelineModel): String = {
      val predictions = pipeline.transform(test)
      predictions.show(truncate=false)

      val evaluator = new RegressionEvaluator()
        .setMetricName("rmse")
        .setLabelCol("ArrDelay")
        .setPredictionCol("prediction")
      val rmse = evaluator.evaluate(predictions)

      println(s"${name}: Root-mean-square error = $rmse")
      rmse.toString()
    }
  }
}



