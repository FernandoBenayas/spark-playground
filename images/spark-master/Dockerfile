FROM debian:bullseye

# Pre-requisites
RUN apt-get update -y
RUN apt-get install -y default-jdk wget python3 scala r-base r-base-dev \
procps python3-pip curl

# Installing sbt
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list
RUN curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add
RUN apt-get --allow-insecure-repositories -y update
RUN apt-get install --allow-unauthenticated -y sbt

# Installing python packages
RUN pip install pyspark requests requests_oauthlib

# Delete this after you are done with tests
RUN apt-get install init -y

# Installing Spark
RUN wget https://ftp.cixug.es/apache/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz -P /opt
WORKDIR /opt
RUN tar -xzvf /opt/spark-3.1.2-bin-hadoop3.2.tgz
RUN mv spark-3.1.2-bin-hadoop3.2 spark
ENV PATH "/opt/spark:/opt/spark/bin:/opt/spark/sbin:${PATH}"

# Exposing master Web UI and driver Cluster and WebUI ports
EXPOSE 8080 7077 4040 18080

# Container ENVs are configured in Compose file

# Check SPARK 3.1.2 binaries in docs
# Needed for Java 11: -Dio.netty.tryReflectionSetAccessible=true
WORKDIR /
COPY master-entrypoint.sh /bin
ENTRYPOINT [ "/bin/master-entrypoint.sh" ]