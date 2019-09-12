FROM rastasheep/ubuntu-sshd:18.04
RUN apt-get -y update --fix-missing

RUN apt-get -y install sudo rsync
