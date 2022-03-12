// Using org.apache.spark.sql so we can accesss internal methods
package org.apache.spark.sql

import org.apache.spark.sql.streaming.{OutputMode}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window}
import org.apache.spark.storage.StorageLevel

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

        val s = StructType(
            List(
                StructField("timestamp", TimestampType, false),
                StructField("value", LongType, false),
                StructField("index", LongType, false)
            )
        )

        /*
        First we create the Spark Session, and register any UDF/UDAFs (if we created any)
        */
        val sparkSession = SparkSession.builder()
            .appName("structuredStreamingHTTP")
            .master("spark://spark-master:7077")
            .config("spark.submit.deployMode", "cluster")
            .getOrCreate
        sparkSession.udf.register("harmonicMean", functions.udaf(HarmonicMean))
        
        /*
        Then we create the input DataFrame
        Notes:
            - Watermark: very important. It allows us to define the name of the column
            to be used as eventTime ("timestamp" in this case) and the maximum lateness
            of the entries (2 minutes in this case). Lateness is important for discarding 
            the storage of late entries in a micro-batch
        */
        val r = sparkSession
            .readStream
            .format("org.apache.spark.sql.APICustomSource")
            .schema(s)
            .load()
            .withWatermark("timestamp", "2 minutes")

        /*
        Now we do some processing. First we create the Window where we want to do the aggregation,
        then we code the aggregation. 
        We'll begin with simple aggregations. Then we will do some UDAF stuff.
        Notes:  
            - Check https://spark.apache.org/docs/3.1.2/api/scala/org/apache/spark/sql/functions$.html#window(timeColumn:org.apache.spark.sql.Column,windowDuration:String,slideDuration:String):org.apache.spark.sql.Column
            for the window API (and general Spark SQL functions API)
            - Check https://spark.apache.org/docs/3.1.2/sql-ref-functions-udf-aggregate.html for a quick UDAF intro.
        */
        val windowDF = window(r.col("timestamp"), "1 minute", "1 minute")
        val dfSimpleAgg = r.groupBy(windowDF)
            .agg(
                sum("value").as("sum_value"),
                max("value").as("max_value"),
                min("value").as("min_value"),
                avg("value").as("avg_value"),
                stddev_pop("value").as("stddev_value")
            )
        r.createOrReplaceTempView("timeseries")
        val dfUDAF = sparkSession.sql("SELECT harmonicMean(*) AS val_harmonicmean FROM timeseries")

        /* Finally, we write the resulting DataFrame.
        We can choose between 3 output modes
            - Complete: the entire table (as it grows) is written
                beware: "join" is not supported when output mode is Complete
                beware: not supported when no aggregations are preformed
            - Append: only new entries are written
                beware: not supported when aggregations are performed
            - Update: only updated entries are written 
                if we don't use aggregations, it's like Append mode
        */
        dfUDAF
            .writeStream
            .format("console")
            .outputMode(OutputMode.Complete())
            .start()
            .awaitTermination()

    }
}