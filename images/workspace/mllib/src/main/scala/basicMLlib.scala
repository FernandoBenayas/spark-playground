package fbds.example.json

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, Dataset, Encoders}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.stat.{Summarizer}
import org.apache.spark.ml.regression.{LinearRegression}

import scala.io.Source._
import org.json4s.jackson.JsonMethods.parse
import org.joda.time.DateTime

import org.json4s.DefaultFormats

object WindowStats {

    def main(args: Array[String]) {

        val spark = SparkSession.builder()
            .appName("schemaJSONExample")
            .master("spark://spark-master:7077")
            .config("spark.submit.deployMode", "cluster")
            .getOrCreate

        val customSchemaBis = StructType(
            Array(
                StructField("description", StringType, true),
                StructField("events", ArrayType(
                    StructType(
                        Array(
                            StructField("categories", ArrayType(
                                StructType(
                                    Array(
                                        StructField("id", StringType, true),
                                        StructField("title", StringType, true)
                                    )
                                ), true),
                            true), 
                            StructField("closed", StringType, true),
                            StructField("description", StringType, true),
                            StructField("geometry", ArrayType(
                                StructType(
                                    Array(
                                        StructField("coordinates", ArrayType(
                                            StringType, true), 
                                        true),
                                        StructField("date", DateType, true),
                                        StructField("magnitudeUnit", StringType, true),
                                        StructField("magnitudeValue", DoubleType, true),
                                        StructField("type", StringType, true)
                                    )
                                ),
                            true), true),
                            StructField("id", StringType, true),
                            StructField("link", StringType, true),
                            StructField("sources", ArrayType(
                                StructType(
                                    Array(
                                        StructField("id", StringType, true),
                                        StructField("url", StringType, true)
                                    )
                                ),
                            true), true),
                            StructField("title", StringType, true)
                        )
                    ),
                true), true),
                StructField("link", StringType, true),
                StructField("title", StringType, true)
            )
        )

        val linerReg = new LinearRegression()
            .setMaxIter(10)
            .setRegParam(0.3)
            .setElasticNetParam(0.8)
            .featuresCol("magnitudeValue")
        
        val eventsDF = spark.read.schema(customSchemaBis).option("multiline", true).json("/opt/eonet_api.json")
        eventsDF.printSchema()

        val explodedEventsDF = eventsDF.select(col("description"), col("link"), col("title"), explode(col("events")).as("event"))
        val explodedDataDF = explodedEventsDF.select(col("event"), explode(col("event.geometry")).as("geometry"))
            .select(col("event.id").as("event"), col("event.categories").as("category"), col("geometry.date"), col("geometry.magnitudeValue"))
            .na.drop(Seq("magnitudeValue"))
        // You can lag values using the window operator
        val sortedDataDF = explodedDataDF.select(col("event"), col("category.id"), col("date"), col("magnitudeValue"), lag("magnitudeValue", 1).over(windowSpec)).show(100)

    }
}

