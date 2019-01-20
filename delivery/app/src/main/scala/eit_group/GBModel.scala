package eit_group

import SimpleModelObject._
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer, OneHotEncoderEstimator, StringIndexer}
import org.apache.spark.ml.regression.{GBTRegressor, GBTRegressionModel}

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
        .setMaxIter(10)

      // Chain indexer and GBT in a Pipeline.
      val pipeline = new Pipeline()
        .setStages(Array(monthIndexer, dayIndexer, encoder, assembler,featureIndexer, gbt))

      // Train model. This also runs the indexer.
      val model = pipeline.fit(training)

//      val gbtModel = model.stages(2).asInstanceOf[GBTRegressionModel]
//      println("Learned regression GBT model:\n" + gbtModel.toDebugString)

      model
    }
  }
}