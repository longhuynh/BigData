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
# bash/run_hadoop_job.sh cs522-2016-hadoop/target/cs522-2016-hadoop-0.0.1-SNAPSHOT.jar wordcount com.myhadoop.jobs.WordCountDriver input/crf_input
#
# BASIC-STRESS-TEST:
# bash/run_hadoop_job.sh cs522-2016-hadoop/target/cs522-2016-hadoop-0.0.1-SNAPSHOT.jar wordcount com.myhadoop.jobs.WordCountDriver input/st_input
#
# BASIC-FORMAT-OUTPUT: 
# bash/run_hadoop_job.sh cs522-2016-hadoop/target/cs522-2016-hadoop-0.0.1-SNAPSHOT.jar wordcount com.myhadoop.formatoutput.jobs.WordCountDriver input/crf_input
#
# 
# PAIR: 
# bash/run_hadoop_job.sh cs522-2016-hadoop/target/cs522-2016-hadoop-0.0.1-SNAPSHOT.jar wordcountpaircrf com.myhadoop.jobs.PairCrfDriver input/crf_input
#
# PAIR-STRESS-TEST:
# bash/run_hadoop_job.sh cs522-2016-hadoop/target/cs522-2016-hadoop-0.0.1-SNAPSHOT.jar wordcountpaircrf com.myhadoop.jobs.PairCrfDriver input/st_input
#
# PARI-FORMAT-OUTPUT: 
# bash/run_hadoop_job.sh cs522-2016-hadoop/target/cs522-2016-hadoop-0.0.1-SNAPSHOT.jar wordcountpaircrf com.myhadoop.formatoutput.jobs.PairCrfDriver input/crf_input
#
#
# STRIPE: 
# bash/run_hadoop_job.sh cs522-2016-hadoop/target/cs522-2016-hadoop-0.0.1-SNAPSHOT.jar wordcountstripecrf com.myhadoop.jobs.StripeCrfDriver input/crf_input
#
# STRIPE-STRESS-TEST:
# bash/run_hadoop_job.sh cs522-2016-hadoop/target/cs522-2016-hadoop-0.0.1-SNAPSHOT.jar wordcountstripecrf com.myhadoop.jobs.StripeCrfDriver input/st_input
#
# STRIPE-FORMAT-OUTPUT: 
# bash/run_hadoop_job.sh cs522-2016-hadoop/target/cs522-2016-hadoop-0.0.1-SNAPSHOT.jar wordcountstripecrf com.myhadoop.formatoutput.jobs.StripeCrfDriver input/crf_input
#
#
# HYBRID: 
# bash/run_hadoop_job.sh cs522-2016-hadoop/target/cs522-2016-hadoop-0.0.1-SNAPSHOT.jar wordcounthybridcrf com.myhadoop.jobs.HybridCrfDriver input/crf_input
#
# HYBRID-STRESS-TEST:
# bash/run_hadoop_job.sh cs522-2016-hadoop/target/cs522-2016-hadoop-0.0.1-SNAPSHOT.jar wordcounthybridcrf com.myhadoop.jobs.HybridCrfDriver input/st_input
#
# HYBRID-FORMAT-OUTPUT: 
# bash/run_hadoop_job.sh cs522-2016-hadoop/target/cs522-2016-hadoop-0.0.1-SNAPSHOT.jar wordcounthybridcrf com.myhadoop.formatoutput.jobs.HybridCrfDriver input/crf_input
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

# View all output files
hadoop fs -cat $outputPath/*
