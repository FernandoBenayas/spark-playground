package fbds.example.json

import java.io.{BufferedReader, InputStreamReader}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, Dataset, Encoders}
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
        

    }
}