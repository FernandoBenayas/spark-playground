#!/bin/bash

start-slave.sh spark://spark-master:7077
tail -f /opt/spark/logs/spark--org.apache.spark.deploy.worker.Worker*.out
