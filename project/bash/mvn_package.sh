#!/bin/bash

#export JAVA_HOME=/home/cloudera/java/jdk1.8.0_77
#export M2_HOME=/home/cloudera/softwares/apache-maven-3.3.9
#export PATH=$JAVA_HOME/bin:$M2_HOME/bin:$PATH

packPath=$1

cd $packPath
mvn clean

mvn package

cd ..
