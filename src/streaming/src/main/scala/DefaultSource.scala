// Using org.apache.spark.sql so I can accesss internal methods
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.execution.streaming.{Source, LongOffset, SerializedOffset, Offset}
import org.apache.spark.sql.sources.{StreamSourceProvider, DataSourceRegister}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.plans.logical.{Range, RepartitionByExpression}
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema}

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import org.apache.http.HttpResponse
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import scala.io.Source._
import org.json4s.jackson.JsonMethods.parse
import org.json4s.DefaultFormats
import org.joda.time.DateTime

// class Datapoint(val timestamp: String, val value: Integer) {
//     // alias for the type to convert to and from
//     type DatapointEncoded = (String, Integer)

//     // implicit conversions
//     implicit def toEncoded(o: Datapoint): DatapointEncoded = (o.timestamp, o.value)
//     implicit def fromEncoded(e: DatapointEncoded): Datapoint = new Datapoint(e._1, e._2)
// }

case class Datapoint(val timestamp: String, val value: Integer)

class DefaultSource extends StreamSourceProvider with DataSourceRegister {
    override def shortName(): String = "CustomSource"
    override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType], providerName: String, parameters: Map[String, String]): (String, StructType) = {
        (shortName(), CustomSource.schema)
    }
    override def createSource(sqlContext: SQLContext, metadataPath: String, schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {
        CustomSource(sqlContext)
    }
}

// REMEMBER: send stuff to the companion object
// Is the offset considered as specific to each instance of the class? I would think so
class CustomSource private (sqlContext: SQLContext) extends Source {

    override def schema: StructType = CustomSource.schema
    private var currentOffset: LongOffset = LongOffset(0)
    private var dataThread: Thread = dataGenerationThread()
    private var batches = collection.mutable.ListBuffer.empty[(String, Long)]
    

    override def getOffset: Option[Offset] = {
        if (currentOffset.offset <= 0) None else Some(currentOffset)
    }

    private def getOffsetValue(offset: Offset): Long = {
      offset match {
        case s: SerializedOffset => LongOffset(s).offset
        case l: LongOffset => l.offset
        case _ => throw new IllegalArgumentException("incorrect offset type: " + offset)
      }
    }

    override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
        val s = start.map(getOffsetValue) getOrElse LongOffset(0).offset
        val e = getOffsetValue(end)

        var plan = Range(
                s,
                e,
                1,
                Some(sqlContext.sparkSession.sparkContext.defaultParallelism),
                isStreaming = true)

        // I'm parallelizing the collection, performing the operation and de-parallelizing again
        // ALWAYS REMEMBER this regarding parallel collections
        // 1. Side-effecting operations can lead to non-determinism
        //  1a. Even if the operation is associative and commutative, a Data Race can result in using as basis old values
        // 2. Non-associative operations lead to non-determinism
        val data = batches
            .par
            .filter { case (_, idx) => idx >= s && idx <= e }
            .seq
        
        val rdd = sqlContext
            .sparkContext
            .parallelize(data)
            .map { case (v, idx) => InternalRow(UTF8String.fromString(v), idx.toLong)}
        
        sqlContext.sparkSession.internalCreateDataFrame(rdd, schema, isStreaming = true)
    }

    // "commit" implements how Spark controls that data with offsets less or equal to
    // the one being passed is not requested. And one way of doing this is deleting such data.
    // Thus, "commit" deletes data with offsets less than the one being passed 
    override def commit(end: Offset): Unit = {
        val commitedOffset = getOffsetValue(end)
        val toKeep = batches
            .filter { case (_, idx) => idx >= commitedOffset }
        batches = toKeep
    }

    override def stop(): Unit = dataThread.stop()

    // Had to leave out Futures for the moment due to **concurrency** issues with Offset (multiple threads modifying it)
    private def dataGenerationThread() = {
        val poller = new Thread("poller") {
            override def run() {
                while (true) {
                    val httpClient = new DefaultHttpClient()
                    val request = new HttpGet("http://dataserver:9090")
                    //val request = new HttpGet("http://www.randomnumberapi.com/api/v1.0/random")
                    request.addHeader("accept", "application/json")
                    val response = httpClient.execute(request)
                    if (response.getStatusLine().getStatusCode() != 200) {
                        throw new RuntimeException("Failed : HTTP error code : " + response.getStatusLine().getStatusCode())
                    }
                    val reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))
                    val line = reader.readLine
                    reader.close
                    httpClient.getConnectionManager().shutdown()

                    this.synchronized {
                        currentOffset += 1
                        batches.append((line, currentOffset.offset))
                    }

                    Thread.sleep(200)
                }
            }
        }
        poller.start()
        poller
    }
}

// Using a companion object since we want our custom source to be as flexible as possible
// "Use a companion object for methods and values which are not specific to instances of the companion class"
object CustomSource {
    def apply(sqlContext: SQLContext): Source = new CustomSource(sqlContext)
    lazy val schema = StructType(
        List(
            StructField("timestamp", StringType, false),
            StructField("value", StringType, false)
        )
    )
    // lazy val schema = StructType(List(StructField("entry", StringType, false)))
}