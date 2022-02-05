FROM debian:bullseye

# Pre-requisites
RUN apt-get update -y
RUN apt-get install default-jdk wget python3 scala r-base r-base-dev \
procps python3-pip -y
RUN pip install pyspark requests requests_oauthlib
# Delete this after you are done with tests
RUN apt-get install init -y

# Installing Spark
RUN wget https://ftp.cixug.es/apache/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz -P /opt
WORKDIR /opt
RUN tar -xzvf /opt/spark-3.1.2-bin-hadoop3.2.tgz
RUN mv spark-3.1.2-bin-hadoop3.2 spark
ENV PATH "/opt/spark:/opt/spark/bin:/opt/spark/sbin:${PATH}"

# Exposing worker Web UI and regular ports
EXPOSE 8081 7077

# Container ENVs are configured in Compose file

# Check SPARK 3.1.2 binaries in docs
# Needed for Java 11: -Dio.netty.tryReflectionSetAccessible=true
WORKDIR /
COPY worker-entrypoint.sh /bin
ENTRYPOINT [ "/bin/worker-entrypoint.sh" ]