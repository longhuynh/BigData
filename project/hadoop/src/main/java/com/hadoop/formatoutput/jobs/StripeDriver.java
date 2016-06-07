package com.hadoop.formatoutput.jobs;

import com.hadoop.dto.SortedStripe;
import com.hadoop.dto.Stripe;
import com.hadoop.formatoutput.reducers.StripeReducer;
import com.hadoop.mappers.StripeMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StripeDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		if (args.length != 2) {
			System.out.printf("Usage: StubDriver <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		Job job = new Job(conf, "wordcountstripe");
		job.setJarByClass(PairDriver.class);
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setMapperClass(StripeMapper.class);
		job.setReducerClass(StripeReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Stripe.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(SortedStripe.class);
		
		// delete output if exits
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputPath)) {
			hdfs.delete(outputPath, true);
		}

		job.waitForCompletion(true);
	}
}
