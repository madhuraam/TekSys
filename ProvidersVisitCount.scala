import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


// Define struct schema for Provider
val providerSchema = StructType(
  Seq(
    StructField("provider_id", StringType, nullable = false),
    StructField("provider_specialty", StringType, nullable = false),
    StructField("first_name", StringType, nullable = false),
    StructField("middle_name", StringType, nullable = false),
    StructField("last_name", StringType, nullable = false)
  )
)

// Define struct schema for Visit
val visitSchema = StructType(
  Seq(
    StructField("visit_id", StringType, nullable = false),
    StructField("provider_id", StringType, nullable = false),
    StructField("visit_date", StringType, nullable = false)
  )
)

// Define case classes for Provider and Visit
case class Provider(provider_id: String, provider_specialty: String, first_name: String, middle_name: String, last_name: String)
case class Visit(visit_id: String, provider_id: String, visit_date: String)

object VisitCalculator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("VisitCalculator")
      .getOrCreate()

    val providerPath = "filepath/providers.csv"
    val visitPath = "filepath/visits.csv"

    // Read CSV files into DataFrames
    val providerDF = spark.read.schema(providerSchema).option("header", "true").option("sep", "|").csv(providerPath).as[Provider]

    def readCSV(fileName: String): List[String] = { 
      spark.read.textFile(fileName).collect().toList
    }

    def parseVisits(visitData: List[String]): List[Visit] = {
      visitData.drop(1).map { line =>
        val Array(visit_id, provider_id, visit_date) = line.split(",")
        Visit(visit_id, provider_id, visit_date)
      }
    }

    val visitsData = readCSV(visitPath)
    val visitsList = parseVisits(visitsData)
    val visitDF = spark.createDataFrame(visitsList)

    // Extract month from the visit date
    val visitDFWithMonth = visitDF.withColumn("month", month(to_date($"visit_date")))

    // Group by provider ID and month, and count visits
    val visitMonthCounts = visitDFWithMonth.groupBy("provider_id", "month").agg(count("*").alias("total_visits"))

    visitMonthCounts.show

    visitMonthCounts.coalesce(1).write.mode("Overwrite").json("/outputpath/")

    val joinedDF = providerDF.join(visitDF, "provider_id")

    // Group by provider ID, name, and specialty, and count visits
    val visitCounts = joinedDF.groupBy("provider_id", "first_name", "middle_name", "last_name", "provider_specialty")
      .agg(count("visit_id").alias("visit_count"))

    // Show the result
    visitCounts.show()

    visitCounts.coalesce(1).write.mode("Overwrite").json("/outputpath/")

       spark.stop()
  }
}
 
