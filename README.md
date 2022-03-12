# spark-playground
Docker-based Spark Playground for distribution-agnostic people

## How-to 

1. Write your own Scala program (or use any of the ones provided inside the **spark-master** image)
2. Compile and package the program (`sbt package`)
3. Send it to the Spark cluster

Example:
```console
spark-submit --conf spark.driver.memory=1g --conf spark.driver.cores=1 --conf spark.executor.memory=2g --conf spark.executor.cores=2 --conf spark.executor.instances=2 --class org.apache.spark.sql.structuredStreamingHTTP --master spark://spark-master:7077 target/scala-2.12/use-spark-structured-streaming_2.12-1.0.jar
```