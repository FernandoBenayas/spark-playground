package fbds.example.json

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, Dataset, Encoders}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.evaluation.{RegressionEvaluator}
import org.apache.spark.ml.stat.{Summarizer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.regression.{LinearRegression, GeneralizedLinearRegression, RandomForestRegressor}
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}

import scala.io.Source._
import org.json4s.jackson.JsonMethods.parse
import org.joda.time.DateTime

import org.json4s.DefaultFormats

// ML using pipelines
object pipelineMLCSV {

    def main(args: Array[String]) {

        val spark = SparkSession.builder()
            .appName("basicMllibExample")
            .master("spark://spark-master:7077")
            .config("spark.submit.deployMode", "cluster")
            .getOrCreate

        val customSchema = StructType(
            Array(
                StructField("Name", StringType, true),
                StructField("Timestamp", IntegerType, true),
                StructField("FloatValue", FloatType, true),
                StructField("IntegerValue", IntegerType, true),
                StructField("Parameter", StringType, true),
                StructField("Satellite", StringType, true),
                StructField("ShortDesc", StringType, true),
                StructField("Units", StringType, true)
            )
        )
        
        val eventsDF = spark.read.schema(customSchema)
            .option("header", "true")
            .option("sep", ",")
            .csv("/opt/csv_acst003i.csv")
            .sort(col("Timestamp"))
        
        // First of all, let's split the dataset into train and test sets (80% - 20%)
        // Under the hood, each executor performs the split on each data partition separatedly.
        // Thus, the same split with the same seed on a cluster with a different number of executors won't yield the same result
        val Array(trainEvents, testEvents) = eventsDF.randomSplit(Array(.8, .2), seed=42)
        
        // Creating Features column; remember Mllib deprecation
        val assembler = new VectorAssembler()
            .setInputCols(Array("Open", "High", "Low"))
            .setOutputCol("Features")

        // Standardising features
        val scaler = new StandardScaler()
            .setInputCol("Features")
            .setOutputCol("scaledFeatures")
            .setWithStd(true)
            .setWithMean(true)

        // Simple Regression
        val linearReg = new LinearRegression()
            .setMaxIter(10)
            .setRegParam(0.3)
            .setElasticNetParam(0.8)
            .setFeaturesCol("Features")
            .setLabelCol("Close")

        // Now let's build the pipeline
        // Using pipelines eases applying the same process for different datasets
        // Besides, Pipeline detects which stages are Transformers or Estimators, so you don't have to guess if .fit or .transform applies
        val pipeline_stages = Array(assembler, scaler, linearReg)
        val pipeline = new Pipeline().setStages(pipeline_stages)
        val pipedModel = pipeline.fit(trainEvents)

        // Now let's use it to predict and evaluate the model over the test dataset
        // Evaluator of the regression
        val evalReg = new RegressionEvaluator()
            .setPredictionCol("prediction")
            .setMetricName("r2")
            .setLabelCol("Close")
        
        // Predict data
        val evalDF = pipedModel.transform(testEvents)
        val r2 = evalReg.evaluate(evalDF)
        println(f"R2 is ${r2}")

    }
}