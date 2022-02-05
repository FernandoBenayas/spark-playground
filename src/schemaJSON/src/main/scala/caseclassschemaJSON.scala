package fbds.example.json

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, Dataset, Encoders}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkContext, SparkConf}

import java.sql.{Date}
import scala.io.Source._
import org.json4s.jackson.JsonMethods.parse
import org.joda.time.DateTime

import org.json4s.DefaultFormats

case class Category(
    id:String,
    title:String
)

case class Source(
    id:String,
    url:String
)

case class Geometry(
    magnitudeValue:Float,
    magnitudeUnit:String,
    date:Date,
    `type`:String,
    coordinates:Array[Double]
)

case class Event(
    id:String,
    title:String,
    link:String,
    closed:String,
    categories:Array[Category],
    sources:Array[Source],
    geometry:Array[Geometry]
)

case class EventsAgg(
    title:String,
    description:String,
    link:String,
    events:Array[Event]
)

object CaseClassSchemaJSON {

    def main(args: Array[String]) {

        val spark = SparkSession.builder()
            .appName("queryDataExample")
            .master("spark://spark-master:7077")
            .config("spark.submit.deployMode", "cluster")
            .getOrCreate
        import spark.implicits._
        
        val encoderSchema = Encoders.product[EventsAgg].schema
        val eventsDF = spark.read.schema(encoderSchema).option("multiline", true).json("/opt/eonet_api.json")
        eventsDF.printSchema()
        
        val explodedEventsDF = eventsDF.select(col("description"), col("link"), col("title"), explode(col("events")).as("event"))
        explodedEventsDF.select(col("event"), explode(col("event.geometry")).as("geometry")).select(col("event.id"), col("geometry.date"), col("geometry.magnitudeValue")).show()

    }
}

