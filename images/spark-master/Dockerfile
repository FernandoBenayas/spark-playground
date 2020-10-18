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

# Exposing master Web UI and driver Cluster and WebUI ports
EXPOSE 8080 7077 4040

# Configure Spark ENVS
ENV PYSPARK_PYTHON python3
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_WEBUI_PORT 8080

# Check SPARK 3.0.1 binaries in docs
# Needed for Java 11: -Dio.netty.tryReflectionSetAccessible=true
WORKDIR /
CMD ["/sbin/init"]