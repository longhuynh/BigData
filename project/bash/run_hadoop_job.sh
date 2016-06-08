#!/bin/bash

############################################
#   MySH/pre_run_hadoop_job.sh
############################################
#sudo su hdfs
#hadoop fs -mkdir /user/cloudera
#hadoop fs -chown cloudera /user/cloudera
#exit
############################################

############################################
#
# Run Hadoop jobs
#
############################################
#
# Template: run_hadoop_job.sh jarPath jobName classDriver inputFilesPath
#
############################################
#
# BASIC: 
# bash/run_hadoop_job.sh hadoop/target/hadoop-0.0.1-SNAPSHOT.jar wordcount com.hadoop.jobs.WordCountDriver input/input
#
# BASIC-FORMAT-OUTPUT: 
# bash/run_hadoop_job.sh hadoop/target/hadoop-0.0.1-SNAPSHOT.jar wordcount com.hadoop.formatoutput.jobs.WordCountDriver input/input
#
# 
# PAIR: 
# bash/run_hadoop_job.sh hadoop/target/hadoop-0.0.1-SNAPSHOT.jar wordcountpair com.hadoop.jobs.PairDriver input/input
#
# PARI-FORMAT-OUTPUT: 
# bash/run_hadoop_job.sh hadoop/target/hadoop-0.0.1-SNAPSHOT.jar wordcountpair com.hadoop.formatoutput.jobs.PairDriver input/input
#
#
# STRIPE: 
# bash/run_hadoop_job.sh hadoop/target/hadoop-0.0.1-SNAPSHOT.jar wordcountstripe com.hadoop.jobs.StripeDriver input/input
#
# STRIPE-FORMAT-OUTPUT: 
# bash/run_hadoop_job.sh hadoop/target/hadoop-0.0.1-SNAPSHOT.jar wordcountstripe com.hadoop.formatoutput.jobs.StripeDriver input/input
#
#
# HYBRID: 
# bash/run_hadoop_job.sh hadoop/target/hadoop-0.0.1-SNAPSHOT.jar wordcounthybrid com.hadoop.jobs.HybridDriver input/input
#
# HYBRID-FORMAT-OUTPUT: 
# bash/run_hadoop_job.sh hadoop/target/hadoop-0.0.1-SNAPSHOT.jar wordcounthybrid com.hadoop.formatoutput.jobs.HybridDriver input/input
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
