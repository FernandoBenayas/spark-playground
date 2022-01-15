package fbds.example.json

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors
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

import scala.util.{Success, Failure}
import scala.concurrent._

import org.apache.http.HttpResponse
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient

// Unreliable receiver: an ACK is not sent to the data source
// See https://spark.apache.org/docs/latest/streaming-custom-receivers.html#receiver-reliability for custom receiver reliability
class CustomHTTPReceiver(url: String, path: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK) {
    def onStart() {
        new Thread("HTTP Poller") {
            override def run() {
                while(true) {
                    poll()
                    Thread.sleep(200)
                } 
            }
        }.start()
    }

    def onStop() {
        // Empty - Clean up futures and connections
    }

    private def poll() {
        // First we define the pool of threads for async polling
        implicit val ec = new ExecutionContext {
            val threadPool = Executors.newFixedThreadPool(5)
            def execute(runnable: Runnable) {
                threadPool.submit(runnable)
            }
            def reportFailure(t: Throwable) {}
        }

        // Then we define the Futures that will be doing the actual polling
        val data: Future[String] = Future {
            val httpClient = new DefaultHttpClient()
            val request = new HttpGet(url + path)
            request.addHeader("accept", "application/json")
            val response = httpClient.execute(request)
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new RuntimeException("Failed : HTTP error code : "+ response.getStatusLine().getStatusCode())
            }
            val reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))
            // Reading a line and storing it into the Future
            val line = reader.readLine
            reader.close
            httpClient.getConnectionManager().shutdown()
            line
        }

        // Now we define the callback for the Future
        data onComplete {
            case Success(line) => store(line)
            case Failure(t) => restart("Error performing polling", t)
        }
    }
}

// WARNING - SPARK STREAMING IS BEING DEPRECATED AND SUBSTITUTED BY STRUCTURED STREAMING
// USING SPARK STREAMING ONLY BECAUSE IT ALLOWS NON-EXPERIMENTAL CUSTOM RECEIVERS
// Am I using the times (particularly in Thread) correctly?
// Beware!! One thread is used by the Receiver. Leave threads for the rest of the program

// Notes: steps to create a Spark Streaming app:
// 1. Create sparkContext
// 2. Create streamingContext
// 3. Define input DStreams (every input DStream is associated to a Receiver)
// 4. Define streaming computations
// 5. Start streaming with streamingContext.start()
object streamingHTTP {

    def main(args: Array[String]) {
        val spark = SparkSession.builder()
            .appName("streamingHTTP")
            .master("spark://spark-master:7077")
            .config("spark.submit.deployMode", "cluster")
            .getOrCreate
        val ssc = new StreamingContext(spark.sparkContext, Seconds(3))
        val stream = ssc.receiverStream(new CustomHTTPReceiver("https://api.kanye.rest", "/"))
        val windowedStream = stream.reduceByWindow((a: String, b: String) => (a.concat(b)), Seconds(15), Seconds(3))
        // This "saveAsTextFiles" is the WRONG way to save results to a file. It will just save them in the executor
        // In a real-life scenario, this would be substituted by a code block storing data in a database
        windowedStream.saveAsTextFiles("/home/projects/example")
        windowedStream.print

        ssc.start()
        ssc.awaitTermination()
    }
}