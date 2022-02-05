package org.apache.spark.sql

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, Dataset, Encoders}
import org.apache.spark.sql.streaming.{OutputMode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming

import scala.io.Source._
import org.json4s.jackson.JsonMethods.parse
import org.json4s.DefaultFormats
import org.joda.time.DateTime

import org.apache.http.HttpResponse
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient


// WARNING - CUSTOM SOURCES IN STRUCTURED STREAMING ARE EXPERIMENTAL
object structuredStreamingHTTP {

    def main(args: Array[String]) {

        val sparkSession = SparkSession.builder()
            .appName("structuredStreamingHTTP")
            .master("spark://spark-master:7077")
            .config("spark.submit.deployMode", "cluster")
            .getOrCreate
        
        val s = StructType(
            List(
                StructField("timestamp", TimestampType, false),
                StructField("value", LongType, false),
                StructField("index", LongType, false)
            )
        )

        val r = sparkSession
            .readStream
            .format("org.apache.spark.sql.DefaultSource")
            .schema(s)
            .load()
        
        r.createTempView("w")

        sparkSession
            .sql("select * from w")
            .writeStream
            .format("console")
            .outputMode(OutputMode.Append())
            .start()
            .awaitTermination()

    }
}