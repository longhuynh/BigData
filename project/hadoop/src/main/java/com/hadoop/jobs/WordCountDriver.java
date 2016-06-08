package com.hadoop.jobs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.hadoop.mappers.WordCountMapper;
import com.hadoop.reducers.WordCountReducer;

public class WordCountDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Path outputPath = new Path(args[1]);
		
		Job job = new Job(conf, "wordcount");
		job.setJarByClass(WordCountDriver.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// delete output if exits
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputPath)) {
			hdfs.delete(outputPath, true);
		}

		job.waitForCompletion(true);

	}
}