# spark-playground
Docker-based Spark Playground for distribution-agnostic people

## How-to 

1. Write your own Scala program (or use any of the ones provided inside the **workspace** image)
2. Compile the program
3. Package the program
4. Send it to the Spark cluster

Example:
```console
spark-submit --conf spark.driver.memory=1g --conf spark.driver.cores=1 --conf spark.executor.memory=2g --conf spark.executor.cores=2 --conf spark.executor.instances=2 --class fbds.example.json.structuredStreamingHTTP --master spark://spark-master:7077 target/scala-2.12/use-spark-streaming_2.12-1.0.jar
```