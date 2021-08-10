import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, Dataset}
import org.apache.spark.{SparkContext, SparkConf}

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
    magnitudeValue:String,
    magnitudeUnit:String,
    date:String,
    `type`:String,
    coordinates: Either[Array[Double], Array[Array[Array[Double]]]]
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

object Main {

    def main(args: Array[String]) {

        val spark = SparkSession.builder()
            .appName("queryDataExample")
            .master("spark://spark-master:7077")
            .config("spark.submit.deployMode", "cluster")
            .getOrCreate
        
        val sc = spark.SparkContext

        // Dont infer the schema!!!
        // sc.addFile('https://eonet.sci.gsfc.nasa.gov/api/v3/events')
        val events_df = spark.read.option("multiline", true).json("/opt/eonet_api.json").as[EventsAgg]
        events_df.printSchema()


        // https://www.alphavantage.co/query?function=FX_DAILY&from_symbol=EUR&to_symbol=ARS&apikey=MRJ8RW7VCFZW9QIP
        
        // use createGlobalTempView("jsonDF") if you also want to use SQL to manage the JSON object


    }

}

