from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType,IntegerType,StringType,StructField,StructType
from tensorflowonspark import TFCluster, TFNode
import tensorflow as tf
import numpy as np

def main_tfo(args, ctx):

    strategy = tf.distribute.MultiWorkerMirroredStrategy()

    def lstm_model_compile():
        lstm_model = tf.keras.models.Sequential([
            tf.keras.layers.LSTM(HISTORY_LAG, input_shape=X_train.shape[-2:]),
            tf.keras.layers.Dense(FUTURE_TARGET)
        ])

        lstm_model.compile(optimizer='adam', metrics=['mae'], loss='mse')
        return lstm_model

    # 'True' because the DataFeed is expecting an output (inferencing)
    tf_feed = TFNode.DataFeed(ctx.mgr, True)

    def rdd_generator():
        while not tf_feed.should_stop():
            batch = tf_feed.next_batch(1)
            if len(batch) > 0:
                example = batch[0]
                data = np.array(example[0]).astype(np.float32)
                data = np.reshape(data, (50, 1))
                value = np.array(example[1]).astype(np.float32)
                value = np.reshape(value, (1, 1))
                yield (data, value)
            else:
                return

    ds = tf.data.Dataset.from_generator(rdd_generator, (tf.float32, tf.float32), (tf.TensorShape([50, 1]), tf.TensorShape([1, 1])))
    ds = ds.batch(args.batch_size)

    lstm_model.fit(X_train, y_train, epochs=EPOCHS)


if __name__ == '__main__':
    ss = SparkSession.builder() \
        .appName("distributed_lstm") \
        .master("spark://spark-master:7077") \
        .config("spark.submit.deployMode", "cluster") \
        .getOrCreate()
    executors = ss._conf.get("spark.executor.instances")
    num_executors = int(executors) if executors is not None else 1

    csvSchema = StructType(
        [
            StructField("Name", StringType(), True),
            StructField("Timestamp", IntegerType(), True),
            StructField("FloatValue", FloatType(), True),
            StructField("IntegerValue", IntegerType(), True),
            StructField("Parameter", StringType(), True),
            StructField("Satellite", StringType(), True),
            StructField("ShortDesc", StringType(), True),
            StructField("Units", StringType(), True)
        ]
    )

    raw_data = ss.read.format("csv") \
        .schema(csvSchema) \
        .option("header", True) \
        .option("delimiter", ",") \
        .load("/opt/csv_acst003i.csv") \
        .sort(col("Timestamp"))
    
    cluster = TFCluster.run(ss.sparkContext, main_fun, args, args.cluster_size, num_ps=0, tensorboard=args.tensorboard, input_mode=TFCluster.InputMode.SPARK, master_node='chief')