FROM ubuntu:bionic

################################################
# Ubuntu repository configuration
################################################

RUN apt-get -y update --fix-missing

# Tools required for adding repositories
RUN apt-get -y install apt-transport-https \
     ca-certificates \
     curl \
     gnupg2 \
     software-properties-common

# Add docker repository
RUN curl -fsSL https://download.docker.com/linux/$(. /etc/os-release; echo "$ID")/gpg > /tmp/dkey; apt-key add /tmp/dkey
RUN add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/$(. /etc/os-release; echo "$ID") \
   $(lsb_release -cs) \
   stable"

# Add postgresql repository
RUN curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc > /tmp/dkey; apt-key add /tmp/dkey
RUN add-apt-repository \
    "deb http://apt.postgresql.org/pub/repos/apt/ \
    $(lsb_release -cs)-pgdg \
    main"

RUN apt-get -y update --fix-missing

################################################
# Ubuntu software installation
################################################

RUN apt-get -y install kmod iproute2
RUN apt-get -y install socat vim rsync sshpass jq
RUN apt-get -y install openjdk-11-jre-headless
RUN apt-get -y install lsof
RUN apt-get -y install docker-ce
RUN DEBIAN_FRONTEND=noninteractive apt-get -y install tzdata
RUN apt-get -y install postgresql-12 postgresql-client-12

################################################
# Titan software installation and configuration
################################################

RUN DEBIAN_FRONTEND=noninteractive apt-get -y install tzdata
RUN apt-get -y install postgresql-10

COPY build/libs/titan-server.jar /titan/
COPY src/scripts/* /titan/

RUN /titan/get-userland

RUN echo 'alias psql="psql postgres://postgres:postgres@localhost/titan"' >> /etc/bash.bashrc

WORKDIR /titan
