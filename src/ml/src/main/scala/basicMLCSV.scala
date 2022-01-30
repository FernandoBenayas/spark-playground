package fbds.example.json

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession, Dataset, Encoders}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.stat.{Summarizer}
import org.apache.spark.ml.evaluation.{RegressionEvaluator}
import org.apache.spark.ml.regression.{LinearRegression, GeneralizedLinearRegression, RandomForestRegressor}
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}

import scala.io.Source._
import org.json4s.jackson.JsonMethods.parse
import org.joda.time.DateTime

import org.json4s.DefaultFormats

// Very simple regression example
object BasicMLCSV {

    def main(args: Array[String]) {

        val spark = SparkSession.builder()
            .appName("basicMllibExample")
            .master("spark://spark-master:7077")
            .config("spark.submit.deployMode", "cluster")
            .getOrCreate

        val customSchema = StructType(
            Array(
                StructField("Date", DateType, true),
                StructField("Open", FloatType, true),
                StructField("High", FloatType, true),
                StructField("Low", FloatType, true),
                StructField("Close", FloatType, true),
                StructField("Volume", IntegerType, true),
                StructField("Name", StringType, true)
            )
        )
        
        val eventsDF = spark.read.schema(customSchema)
            .option("header", "true")
            .option("sep", ",")
            .csv("/opt/AMZN.csv")
            .sort(col("Date"))

        // First of all, let's split the dataset into train and test sets (80% - 20%)
        // Under the hood, each executor performs the split on each data partition separatedly.
        // Thus, the same split with the same seed on a cluster with a different number of executors won't yield the same result
        val Array(trainEvents, testEvents) = eventsDF.randomSplit(Array(.8, .2), seed=42)
        
        // Creating Features column; remember Mllib deprecation
        val assembler = new VectorAssembler()
            .setInputCols(Array("Open", "High", "Low"))
            .setOutputCol("Features")
        val featuresDF = assembler.transform(trainEvents)
        val testFeaturesDF = assembler.transform(testEvents)

        // Standardising features
        val scaler = new StandardScaler()
            .setInputCol("Features")
            .setOutputCol("scaledFeatures")
            .setWithStd(true)
            .setWithMean(true)
        val scaledFeaturesDF = scaler.fit(featuresDF).transform(featuresDF)
        val scaledTestFeaturesDF = scaler.fit(testFeaturesDF).transform(testFeaturesDF)

        // First we test simple Regression
        val linearReg = new LinearRegression()
            .setMaxIter(10)
            .setRegParam(0.3)
            .setElasticNetParam(0.8)
            .setFeaturesCol("Features")
            .setLabelCol("Close")
        
        val model = linearReg.fit(featuresDF)
        println(s"Unscaled Coefficients: ${model.coefficients} Unscaled Intercept: ${model.intercept}")
        val summary = model.evaluate(testFeaturesDF)
        println(s"Unscaled MSE: ${summary.meanSquaredError} Unscaled R2: ${summary.r2adj}")

        // What happens if we use the standardised features?
        val scaledLinearReg = new LinearRegression()
            .setMaxIter(10)
            .setRegParam(0.3)
            .setElasticNetParam(0.8)
            .setFeaturesCol("scaledFeatures")
            .setLabelCol("Close")
        
        val scaledModel = scaledLinearReg.fit(scaledFeaturesDF)
        println(s"Scaled Coefficients: ${scaledModel.coefficients} Scaled Intercept: ${scaledModel.intercept}")
        val scaledSummary = scaledModel.evaluate(scaledTestFeaturesDF)
        println(s"Scaled MSE: ${scaledSummary.meanSquaredError} Scaled R2: ${scaledSummary.r2adj}")

        // What happens if we use other regression models available in Spark?
        // Let's start with GeneralizedLinearRegression
        val glrReg = new GeneralizedLinearRegression()
            .setMaxIter(20)
            .setFeaturesCol("Features")
            .setLabelCol("Close")
            .setFamily("gamma")
        
        val glrModel = glrReg.fit(featuresDF)
        println(s"GLR Coefficients: ${glrModel.coefficients} glr Intercept: ${glrModel.intercept}")
        val glrSummary = glrModel.evaluate(testFeaturesDF)
        println(s"glr Deviance: ${glrSummary.deviance} glr Dispersion: ${glrSummary.dispersion}")

        // What about Random Forest?
        val rfReg = new RandomForestRegressor()
            .setFeaturesCol("Features")
            .setLabelCol("Close")

        val rfModel = rfReg.fit(featuresDF)
        val rfPreds = model.transform(testFeaturesDF)
        
        // Random Forest models can ony be evaluated with a separate evaluator
        val evaluator = new RegressionEvaluator()
            .setLabelCol("Close")
            .setPredictionCol("prediction")
            .setMetricName("r2")
        val rfR2 = evaluator.evaluate(rfPreds)
        println(s"R2 on test data = ${rfR2}")
    }
}

