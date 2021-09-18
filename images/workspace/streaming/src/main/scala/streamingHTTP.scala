package fbds.example.json

import java.io.{BufferedReader, InputStreamReader}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, Dataset, Encoders}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.receiver.{Receiver}

import scala.io.Source._
import org.json4s.jackson.JsonMethods.parse
import org.json4s.DefaultFormats
import org.joda.time.DateTime

import org.apache.http.HttpResponse
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient

class CustomHTTPReceiver(url: String, path: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK) {
    def onStart() {
        new Thread("HTTP Poller") {
            override def run() { poll() }
        }.start()
    }

    def onStop() {
        // Empty - The thread is programmed to stop when isStopped returns true
    }

    private def poll() {
        try {
            val httpClient = new DefaultHttpClient()
            val request = new HttpGet(url + path)
            request.addHeader("accept", "application/json")
            while(!isStopped) {
                val response = httpClient.execute(request)
                if (response.getStatusLine().getStatusCode() != 200) {
                    throw new RuntimeException("Failed : HTTP error code : "+ response.getStatusLine().getStatusCode())
                }
                val reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))
                var dataInput = reader.readLine
                while (dataInput != null) {
                    store(dataInput)
                    dataInput = reader.readLine
                }
                reader.close
                Thread.sleep(60*1000)
            }
        httpClient.close   
        } catch {
            case e: java.net.ConnectException =>
                restart("Error connecting to provided URL", e)
            case t: Throwable =>
                restart("Error receiving data", t)
        }
    }
}

// WARNING - SPARK STREAMING IS BEING LEFT SUBSTITUTED BY STRUCTURED STREAMING
// USING SPARK STREAMING ONLY BECAUSE IT ALLOWS NON-EXPERIMENTAL CUSTOM RECEIVERS
// Am I using the times (particularly in Thread) correctly?
object streamingHTTP {

    def main(args: Array[String]) {
        val spark = SparkSession.builder()
            .appName("streamingHTTP")
            .master("spark://spark-master:7077")
            .config("spark.submit.deployMode", "cluster")
            .getOrCreate
        val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
        val stream = ssc.receiverStream(new CustomHTTPReceiver("https://spark.apache.org", "/docs/latest/structured-streaming-programming-guide.html"))
        stream.saveAsTextFiles("/home/projects/example")
        stream.print
        
        ssc.start()
        ssc.awaitTermination()
    }
}