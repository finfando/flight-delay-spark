package eit_group
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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
      val flightsDF = data
        .filter("Cancelled = 0")
        .filter("Diverted = 0")
        .drop("ArrTime","ActualElapsedTime","AirTime","TaxiIn","Diverted","CarrierDelay","WeatherDelay","NASDelay","SecurityDelay","LateAircraftDelay")
        .drop("Cancelled","Diverted","CancellationCode","TailNum","FlightNum","Year","DayOfMonth")
        .withColumn("Hour", sqlfuncHour(col("CRSDepTime")))
      println("Count after preprocessing")
      println(flightsDF.count())
      // new variable: NightFlight [1,0] (from hour)

      //Transformations

      //Models - MLlib


      //Models validation

      println("*** toString() just gives you the schema")
      println(flightsDF.toString())

      println("*** It's better to use printSchema()")
      flightsDF.printSchema()

      println("*** show() gives you neatly formatted data")
      flightsDF.show()

      println("*** use select() to choose one column")
      flightsDF.select("ArrDelay").show()

      flightsDF.createOrReplaceTempView("flights")

//      val sqlDF = spark.sql("SELECT * FROM people")
//      flightsDF.repartition(1).saveAsTextFile(s"file://${outputPath}")
    }
  }
}