FROM debian:bullseye

# Pre-requisites
RUN apt-get update -y
RUN apt-get install default-jdk wget python3 scala r-base r-base-dev \
 -y
# Delete this after you are done with tests
RUN apt-get install init -y

# Installing Spark
RUN wget http://apachemirror.wuchna.com/spark/spark-3.0.1/spark-3.0.1-bin-hadoop3.2.tgz -P /opt
WORKDIR /opt
RUN tar -xzvf spark-3.0.1-bin-hadoop3.2.tgz
RUN mv spark-3.0.1-bin-hadoop3.2 spark
ENV PATH "/opt/spark:/opt/spark/bin:/opt/spark/sbin:${PATH}"

# Exposing worker Web UI and regular ports
EXPOSE 8081 7078

# Configure Spark ENVS
ENV PYSPARK_PYTHON python3
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_WORKER_PORT 7078
ENV SPARK_WORKER_WEBUI_PORT 8081

# Check SPARK 3.0.1 binaries in docs
# Needed for Java 11: -Dio.netty.tryReflectionSetAccessible=true
WORKDIR /
CMD ["/sbin/init"]