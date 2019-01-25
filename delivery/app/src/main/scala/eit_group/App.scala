package eit_group
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import SimpleModelObject._

object App {
  def main(args : Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    if (args.length < 2) {
      println("please provide inputPath as first parameter and outputPath as second parameter")
    } else {
      val inputPath = args(0)
      val outputPath = args(1)

      val spark =
        SparkSession.builder()
          .appName("DataFrame-Basic")
          .master("local[4]")
          .config("spark.hadoop.validateOutputSpecs","false")
          .getOrCreate()

      // Load the data
      val data = spark.read.format("csv")
        .option("header", "true")
        .option("nullValue","NA")
        .option("nanValue","NA")
        .load("file:///"+inputPath)

      // Data preprocessing
      println("Count before preprocessing")
      println(data.count())

      val hourCoder: String => Int = (arg: String) => {if (arg.length ==1 | arg.length ==2) 0 else if (arg.length ==3) arg.substring(0,1).toInt else arg.substring(0,2).toInt}
      val sqlfuncHour = udf(hourCoder)
      val nightCoder: Int => Int = (arg: Int) => {if (arg <= 4 | arg >= 23) 1 else 0}
      val sqlfuncNight = udf(nightCoder)
      val preprocessedflightsDF = data
        .filter("Cancelled = 0")
        .filter("Diverted = 0")
        .drop("ArrTime","ActualElapsedTime","AirTime","TaxiIn","Diverted","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay")
        .drop("Cancelled","Diverted","CancellationCode","TailNum","FlightNum","Year","DayOfMonth", "Origin", "Dest", "CRSElapsedTime", "CRSArrTime")
        .withColumn("Hour", sqlfuncHour(col("CRSDepTime")))
        .withColumn("NightFlight", sqlfuncNight(col("Hour")))
        .withColumn("DepDelay", col("DepDelay").cast("Double"))
        .withColumn("Distance", col("Distance").cast("Double"))
        .withColumn("TaxiOut", col("TaxiOut").cast("Double"))
        .withColumn("ArrDelay", col("ArrDelay").cast("Double"))

      println("Count after preprocessing")
      println(preprocessedflightsDF.count())

      println("Count after preprocessing and removal of null values")
      val flightsDF = preprocessedflightsDF.na.drop()
      println(flightsDF.count())
      flightsDF.printSchema()
      flightsDF.show()

      val Array(training, test) = flightsDF.randomSplit(Array(0.9, 0.1), seed = 12345)
      println(s"Size of train set: ${training.count()}")
      println(s"Size of test set: ${test.count()}")

      val quantiles = preprocessedflightsDF.stat.approxQuantile("ArrDelay", Array(0.01,0.99),0.0)
      val quantile_bottom = quantiles(0)
      val quantile_top = quantiles(1)
      val training_filtered = training.filter(s"ArrDelay<$quantile_top AND ArrDelay>$quantile_bottom")
      println(s"Dropping observations with ArrDelay under $quantile_bottom and above $quantile_top in training data set (2% of most extreme observations)")
      println(s"Size of train filtered set: ${training_filtered.count()}")

      val linearModel = new SimpleModel("Linear")
      val predictions = linearModel.evaluate(test, linearModel.train(training_filtered))
      predictions.repartition(1)
        .select("ArrDelay","prediction")
        .withColumn("ArrDelay", col("ArrDelay").cast("Integer"))
        .withColumn("prediction", col("prediction").cast("Integer"))
        .write.option("header", "true").csv(s"file:///$outputPath")

      // other models
//      val gbModel = new GBModel("GB")
//      val gbModelPredictions = gbModel.evaluate(test, gbModel.train(training))

//      val forestModel = new ForestModel("Forest")
//      val gbModelPredictions = forestModel.evaluate(test, forestModel.train(training))
    }
  }
}