#!/bin/bash

############################################
#
# Template: run_optional_job.sh jarPath jobName classDriver inputFilesPath
#
############################################
#
# bash/run_optional_job.sh optional/target/optional-0.0.1-SNAPSHOT.jar customer com.hadoop.jobs.CustomerDriver input/customerinput
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
hadoop fs -mkdir $jobPath $jobPath/input

# Copy all input files from somewhere to inputPath
hadoop fs -put $inputFilesPath* $inputPath

# Run Hadoop job
hadoop jar $jarPath $classDriver $inputPath $outputPath

# copyToLocal
# hadoop fs -copyToLocal $outputPath /home/cloudera/$jobName

# View all output files
hadoop fs -cat $outputPath/*
