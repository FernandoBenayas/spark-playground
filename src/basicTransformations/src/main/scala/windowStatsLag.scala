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

object WindowStatsLag {

    def main(args: Array[String]) {

        val spark = SparkSession.builder()
            .appName("basicMllibExample")
            .master("spark://spark-master:7077")
            .config("spark.submit.deployMode", "cluster")
            .getOrCreate

        val customSchemaBis = StructType(
            Array(
                StructField("COD", StringType, true),
                StructField("Nombre", StringType, true),
                StructField("T3_Unidad", StringType, true),
                StructField("T3_Escala", StringType, true),
                StructField("MetaData", ArrayType(
                    StructType(
                        Array(
                            StructField("Id", IntegerType, true),
                            StructField("Variable", StructType(
                                Array(
                                    StructField("Id", IntegerType, true),
                                    StructField("Nombre", StringType, true),
                                    StructField("Codigo", StringType, true)
                                )),
                            true),
                            StructField("Nombre", StringType, true),
                            StructField("Codigo", StringType, true)
                        )
                    ),
                true), true),
                StructField("Data", ArrayType(
                    StructType(
                        Array(
                            StructField("Fecha", TimestampType, true),
                            StructField("T3_TipoDato", StringType, true),
                            StructField("T3_Periodo", StringType, true),
                            StructField("Anyo", ShortType, true),
                            StructField("Valor", FloatType, true)
                        )
                    ),
                true), true)
            )
        )
        
        val eventsDF = spark.read.schema(customSchemaBis).option("multiline", true).json("/opt/mortages_data_es.json")
        val explodedEventsDF = eventsDF.select(col("COD"), col("Nombre"), col("T3_Unidad"), col("T3_Escala"), col("MetaData"), explode(col("Data")).as("Data"))
        // Adding lagged values
        // WARNING!! Don't use just collect_list, it is non-deterministic. Order is not guaranteed. Use Window
        // Use a combination of columns that yields an unique ID for each row when partitioning without wanting to group 
        val windowSpec = Window.partitionBy("COD", "Data").orderBy("Data.Fecha").rowsBetween(-10, -1)
        val laggedEventsDF = explodedEventsDF.select(col("*"), collect_list("Data.Valor").over(windowSpec).as("laggedValues"))
        laggedEventsDF.show(100)

    }
}

