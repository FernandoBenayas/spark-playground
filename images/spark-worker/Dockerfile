FROM debian:bullseye

# Pre-requisites
RUN apt-get update -y
RUN apt-get install default-jdk wget python3 scala r-base r-base-dev \
procps -y
# Delete this after you are done with tests
RUN apt-get install init -y

# Installing Spark
RUN wget http://apachemirror.wuchna.com/spark/spark-3.0.1/spark-3.0.1-bin-hadoop3.2.tgz -P /opt
WORKDIR /opt
RUN tar -xzvf spark-3.0.1-bin-hadoop3.2.tgz
RUN mv spark-3.0.1-bin-hadoop3.2 spark
ENV PATH "/opt/spark:/opt/spark/bin:/opt/spark/sbin:${PATH}"

# Exposing worker Web UI and regular ports
EXPOSE 8081 7077

# Container ENVs are configured in Compose file

# Check SPARK 3.0.1 binaries in docs
# Needed for Java 11: -Dio.netty.tryReflectionSetAccessible=true
WORKDIR /
COPY worker-entrypoint.sh /bin
ENTRYPOINT [ "/bin/worker-entrypoint.sh" ]