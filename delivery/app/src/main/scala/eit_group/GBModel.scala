package eit_group

import SimpleModelObject._
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{GBTRegressor, GBTRegressionModel}

object GBModelObject extends App {
  class GBModel(name: String) extends SimpleModel(name) {
    override def train(training: DataFrame): PipelineModel = {
      val assembler = new VectorAssembler()
        .setInputCols(Array("DepDelay"))//,"Month"
        .setOutputCol("features")
      val output = assembler.transform(training.select("DepDelay","ArrDelay"))
//      output.show(truncate=false)

      val featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexFeatures")
        .setMaxCategories(12)
        .fit(output)

      val gbt = new GBTRegressor()
        .setLabelCol("ArrDelay")
        .setFeaturesCol("indexFeatures")
        .setMaxIter(10)

      // Chain indexer and GBT in a Pipeline.
      val pipeline = new Pipeline()
        .setStages(Array(assembler,featureIndexer, gbt))

      // Train model. This also runs the indexer.
      val model = pipeline.fit(training)

//      val gbtModel = model.stages(2).asInstanceOf[GBTRegressionModel]
//      println("Learned regression GBT model:\n" + gbtModel.toDebugString)

      model
    }
  }
}