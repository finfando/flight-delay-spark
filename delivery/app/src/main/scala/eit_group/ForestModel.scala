package eit_group

import SimpleModelObject._
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{RandomForestRegressor, RandomForestRegressionModel}

object ForestModelObject extends App {
  class ForestModel(name: String) extends SimpleModel(name) {
    override def train(training: DataFrame): PipelineModel = {

      val assembler = new VectorAssembler()
        .setInputCols(Array("DepDelay"))//,"Month"
        .setOutputCol("features")

      val output = assembler.transform(training.select("DepDelay","ArrDelay"))

      val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexFeatures")
        .setMaxCategories(12)
        .fit(output)

      val rf = new RandomForestRegressor()
        .setLabelCol("ArrDelay")
        .setFeaturesCol("indexFeatures")

      val pipeline = new Pipeline()
        .setStages(Array(assembler,featureIndexer, rf))

      // Train model. This also runs the indexer.
      val model = pipeline.fit(training)

//      val rfModel = model.stages(2).asInstanceOf[RandomForestRegressionModel]
//      println("Learned regression forest model:\n" + rfModel.toDebugString)

      model
    }
  }
}