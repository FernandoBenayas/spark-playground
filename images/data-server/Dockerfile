FROM debian:bullseye

# Pre-requisites
RUN apt-get -y update && apt-get -y install python3 \
    curl
RUN mkdir /root/server

# Copying script
WORKDIR /root/server
COPY simpleServer.py /root/server

# Exposing server ports
EXPOSE 9090

WORKDIR /
CMD [ "python3", "/root/server/simpleServer.py"]