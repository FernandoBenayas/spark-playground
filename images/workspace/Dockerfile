FROM debian:bullseye

# Pre-requisites
RUN apt-get update -y && apt-get install -y default-jdk wget python3 scala r-base r-base-dev procps python3-pip curl vim
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list
RUN curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add
RUN apt-get --allow-insecure-repositories -y update
RUN apt-get install --allow-unauthenticated -y sbt
RUN pip3 install pyspark requests requests_oauthlib google google-api-python-client
# Delete this after you are done with tests
RUN apt-get install init -y

# Exposing port in case that you want to interact with somebody
EXPOSE 6006

# Copying app files
COPY schemaJSON /home/projects/schemaJSON
COPY basicTransformations /home/projects/basicTransformations
COPY ml /home/projects/ml
COPY streaming /home/projects/streaming

# Container ENVs are configured in Compose file

# Check SPARK 3.1.2 binaries in docs
# Needed for Java 11: -Dio.netty.tryReflectionSetAccessible=true
WORKDIR /
CMD [ "/sbin/init" ]