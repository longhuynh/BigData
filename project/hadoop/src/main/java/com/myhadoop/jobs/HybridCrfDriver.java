package com.myhadoop.jobs;

import com.myhadoop.dto.Stripe;
import com.myhadoop.mappers.HybridCrfMapper;
import com.myhadoop.reducers.HybridCrfReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.myhadoop.dto.Pair;

public class HybridCrfDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		if (args.length != 2) {
			System.out.printf("Usage: StubDriver <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Job job = new Job(conf, "wordcounthybridcrf");
		job.setJarByClass(HybridCrfDriver.class);
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapperClass(HybridCrfMapper.class);
		job.setReducerClass(HybridCrfReducer.class);
		
		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Stripe.class);
		
		// delete output if exits
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputPath)) {
			hdfs.delete(outputPath, true);
		}

		job.waitForCompletion(true);
	}
}
