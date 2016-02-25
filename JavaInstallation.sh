#!/bin/bash
echo "Starting to Install Java version 8"
add-apt-repository ppa:webupd8team/java
apt-get update
apt-get install oracle-java8-installer -y
echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | sudo /usr/bin/debconf-set-selections
apt-get install oracle-java8-set-default
echo "Completed Java Installation"

apt-get install build-essential -y
apt-get install erlang-base-hipe -y
apt-get install erlang-dev -y
apt-get install erlang-manpages -y
apt-get install erlang-eunit -y
apt-get install erlang-nox -y
apt-get install libicu-dev -y
apt-get install libmozjs-dev -y
apt-get install libcurl4-openssl-dev -y
apt-get install libjna-java -y