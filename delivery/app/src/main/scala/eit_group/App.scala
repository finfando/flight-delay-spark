package eit_group
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler

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
//        .option("nullValue","null")
//        .option("nanValue",1)
        .load("file:///"+inputPath)

      // Data preprocessing
      println("Count before preprocessing")
      println(data.count())

      val hourCoder: (String => String) = (arg: String) => {if (arg.length ==2 ) "0" else if (arg.length ==3) arg.substring(0,1) else arg.substring(0,2)}
      val sqlfuncHour = udf(hourCoder)
      // new variable: NightFlight [1,0] (from hour)
      val flightsDF = data
        .filter("Cancelled = 0")
        .filter("Diverted = 0")
        .drop("ArrTime","ActualElapsedTime","AirTime","TaxiIn","Diverted","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay")
        .drop("Cancelled","Diverted","CancellationCode","TailNum","FlightNum","Year","DayOfMonth")
        .withColumn("Hour", sqlfuncHour(col("CRSDepTime")))
        .withColumn("DepDelay", col("DepDelay").cast("Double"))
        .withColumn("ArrDelay", col("ArrDelay").cast("Double"))

      println("Count after preprocessing")
      println(flightsDF.count())

      println("*** toString() just gives you the schema")
      println(flightsDF.toString())
      println("*** It's better to use printSchema()")
      flightsDF.printSchema()
      println("*** show() gives you neatly formatted data")
      flightsDF.show()
      println("*** use select() to choose one column")
      flightsDF.select("ArrDelay").show()

      val assembler = new VectorAssembler()
        .setInputCols(Array("DepDelay"))
        .setOutputCol("features")
      val output = assembler.transform(flightsDF.select("DepDelay","ArrDelay"))
      output.show(truncate=false)

      val lr = new LinearRegression()
        .setFeaturesCol("features")
        .setLabelCol("ArrDelay")
        .setMaxIter(10)
        .setElasticNetParam(0.8)
      val lrModel = lr.fit(output)
      println(s"Coefficients: ${lrModel.coefficients}")
      println(s"Intercept: ${lrModel.intercept}")
      val trainingSummary = lrModel.summary
      println(s"numIterations: ${trainingSummary.totalIterations}")
      println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
      trainingSummary.residuals.show()
      println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
      println(s"r2: ${trainingSummary.r2}")


      //Models validation

//      flightsDF.createOrReplaceTempView("flights")
//      val sqlDF = spark.sql("SELECT * FROM people")
//      flightsDF.repartition(1).saveAsTextFile(s"file://${outputPath}")
    }
  }
}