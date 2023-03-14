# Start ecflow server
FROM ubuntu:18.04

WORKDIR /ecflow-server

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    ecflow-server \
    ecflow-client

# Install remaining requirements

WORKDIR /srv/ecflow_server
VOLUME /srv/ecflow_server

#EXPOSE 3141
#CMD ["ecflow_start", "-p", "3141", "-d", "/srv/ecflow_server"]

RUN adduser ecflow
WORKDIR /home/ecflow
USER ecflow

EXPOSE 3141

ENTRYPOINT ["ecflow_server", "-d"]
