#!/bin/bash

start-master.sh
tail -f /opt/spark/logs/spark--org.apache.spark.deploy.master.Master*.out
