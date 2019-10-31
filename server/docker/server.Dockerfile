FROM ubuntu:bionic
RUN apt-get -y update --fix-missing
RUN apt-get -y install kmod iproute2
RUN apt-get -y install socat vim curl rsync sshpass jq
RUN apt-get -y install openjdk-11-jre-headless
RUN apt-get -y install lsof

RUN apt-get -y install apt-transport-https \
     ca-certificates \
     curl \
     gnupg2 \
     software-properties-common
RUN curl -fsSL https://download.docker.com/linux/$(. /etc/os-release; echo "$ID")/gpg > /tmp/dkey; apt-key add /tmp/dkey
RUN add-apt-repository \
   "deb [arch=amd64] https://download.docker.com/linux/$(. /etc/os-release; echo "$ID") \
   $(lsb_release -cs) \
   stable"
RUN apt-get -y update --fix-missing
RUN apt-get -y install docker-ce

RUN DEBIAN_FRONTEND=noninteractive apt-get -y install tzdata
RUN apt-get -y install postgresql-10

COPY build/libs/titan-server.jar /titan/
COPY src/scripts/* /titan/

RUN /titan/get-userland

WORKDIR /titan
