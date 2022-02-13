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

/*
Things to check out
    - Window processing (using event time)
    - Check the query plan and look for stateful operations
    - Bound the amount of intermediate in-memory state accumulated - WATERMARKING

Remember:
    - Custom sources are EXPERIMENTAL right now
    - DataFrames and Datasets work with streaming data just as they do with static data
        - Some operations are not supported though:
            * 
    - Use typed DFs whenever possible (we do here)
        - If it can't be done, spark.sql.streaming.schemaInference = true
    - Here we create a TempView and apply sql commands, but we can also just
    execute selects, groupbys (windows), filters... on the DF directly

    
*/

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
            .format("org.apache.spark.sql.DefaultCustomSource")
            .schema(s)
            .load()
            .withWatermark("timestamp", "2 minutes")
        
        r.createTempView("w")

        // Using output in Append mode. Makes the most sense in scenarios were past entries 
        // are not expected to change
        sparkSession
            .sql("select * from w")
            .writeStream
            .format("console")
            .outputMode(OutputMode.Append())
            .start()
            .awaitTermination()

    }
}