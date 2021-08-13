package fbds.example.json

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, Dataset, Encoders}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkContext, SparkConf}

import java.sql.{Timestamp}
import scala.io.Source._
import org.json4s.jackson.JsonMethods.parse
import org.joda.time.DateTime

import org.json4s.DefaultFormats

object AutoSchemaJSON {

    def main(args: Array[String]) {

        val spark = SparkSession.builder()
            .appName("queryDataExample")
            .master("spark://spark-master:7077")
            .config("spark.submit.deployMode", "cluster")
            .getOrCreate
        
        val eventsDF = spark.read.option("multiline", true).json("/opt/eonet_api.json")
        eventsDF.printSchema()

        val explodedEventsDF = eventsDF.select(col("description"), col("link"), col("title"), explode(col("events")).as("event"))
        explodedEventsDF.select(col("event"), explode(col("event.geometry")).as("geometry")).select(col("event.id"), col("geometry.date"), col("geometry.magnitudeValue")).show()
    }
}