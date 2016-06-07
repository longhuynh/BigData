#!/bin/bash

# Set up before running Hadoop jobs
sudo su hdfs
hadoop fs -mkdir /user/cloudera
hadoop fs -chown cloudera /user/cloudera
exit
