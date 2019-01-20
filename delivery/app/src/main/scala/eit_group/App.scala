package eit_group
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import SimpleModelObject._
import GBModelObject._
import ForestModelObject._


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
//        .limit(1000)
//      data.filter(col("LateAircraftDelay").isNull).show()

      // Data preprocessing
      println("Count before preprocessing")
      println(data.count())

      val hourCoder: (String => Int) = (arg: String) => {if (arg.length ==1 | arg.length ==2) 0 else if (arg.length ==3) arg.substring(0,1).toInt else arg.substring(0,2).toInt}
      val sqlfuncHour = udf(hourCoder)
      val nightCoder: (Int => Int) = (arg: Int) => {if (arg <= 4 | arg >= 23) 1 else 0}
      val sqlfuncNight = udf(nightCoder)
      val preprocessedflightsDF = data
        .filter("Cancelled = 0")
        .filter("Diverted = 0")
        .drop("ArrTime","ActualElapsedTime","AirTime","TaxiIn","Diverted","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay")
        .drop("Cancelled","Diverted","CancellationCode","TailNum","FlightNum","Year","DayOfMonth")
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
      println(flightsDF.na.drop().count())

      flightsDF.printSchema()
      flightsDF.show()

      val split = flightsDF.randomSplit(Array(0.7,0.3))
      val training = split(0)
      val test = split(1)

//      val linearModel = new SimpleModel("Linear")
//      val linearModelRMSE = linearModel.evaluate(test, linearModel.train(training))

      val gbModel = new GBModel("GB")
      val gbModelRMSE = gbModel.evaluate(test, gbModel.train(training))

//      val forestModel = new ForestModel("Forest")
//      val forestModelRMSE = forestModel.evaluate(test, forestModel.train(training))

//      println(linearModelRMSE)
      println(gbModelRMSE)
//      println(forestModelRMSE)


//      flightsDF.repartition(1).saveAsTextFile(s"file://${outputPath}")

//      flightsDF.createOrReplaceTempView("flights")
//      val sqlDF = spark.sql("SELECT * FROM people")
    }
  }
}