# spark-playground
Docker-based Spark Playground for distribution-agnostic people

## How-to 

1. Write your own Scala program (or use any of the ones provided inside the **workspace** image)
2. Compile the program
3. Package the program
4. Send it to the Spark cluster

Example:
```console
spark-submit --class fbds.example.json.AutoSchemaJSON --master spark://spark-master:7077 read-json-using-defined-schema_2.12-1.0.jar
```