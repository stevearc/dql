#!/bin/bash

# Adapted from: https://github.com/mansenfranzen/pywrangler/blob/master/travisci/java_install.sh

# nosetests dynamo plugin requires Java 8 in order to work properly.
# However, TravisCI's xenial ships with Java 11 and Java can't be set
# with `jdk` when python is selected as language.



echo "1 ---------------------------------------"
# show current JAVA_HOME and java version
echo "Current JAVA_HOME: $JAVA_HOME"
echo "Current java -version:"
java -version

echo "2 ---------------------------------------"
# install Java 8
sudo add-apt-repository -y ppa:openjdk-r/ppa
sudo apt-get -qq update
sudo apt-get install -y openjdk-8-jdk --no-install-recommends

echo "3 ---------------------------------------"
sudo update-java-alternatives --list
echo "4 ---------------------------------------"
# sudo update-java-alternatives --set /usr/lib/jvm/java-1.8.0-openjdk-amd64
sudo update-java-alternatives --jre-headless -s java-1.8.0-openjdk-amd64
echo "5 ---------------------------------------"
# echo "sudo update-alternatives --config java"
# sudo update-alternatives --config java
# echo "6 ---------------------------------------"

# change JAVA_HOME to Java 8
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
echo "Updated JAVA_HOME: $JAVA_HOME"

echo "7 ---------------------------------------"
java -version
echo "8 ---------------------------------------"
ls -la /usr/lib/jvm/
