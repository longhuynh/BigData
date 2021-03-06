#!/bin/bash

############################################
#
# Run Hadoop Spark jobs
#
############################################
#
# Template: run_spark_job.sh jarPath jobName classDriver inputFilesPath
#
############################################
#
# Example: bash/run_spark_job.sh spark/target/spark-0.0.1-SNAPSHOT.jar wordcountspark com.spark.wordcount.WordCountPair input/input
#
############################################

#sudo su cloudera

# Declare variables
jarPath=$1
jobName=$2
classDriver=$3
inputFilesPath=$4

envPath=/user/cloudera
jobPath=$envPath/$jobName
inputPath=$jobPath/input
outputPath=$jobPath/output

# Remove folders of job, job input and job output
hadoop fs -rm -r $jobPath

# Create folder to store input data and run job
hadoop fs -mkdir $jobPath $inputPath

# Copy all input files from somewhere to inputPath
hadoop fs -put $inputFilesPath* $inputPath

# Run Hadoop job
#hadoop jar $jarPath $classDriver $inputPath $outputPath
spark-submit --class $classDriver --master local $jarPath $inputPath $outputPath

# View all output files
hadoop fs -cat $outputPath/*
