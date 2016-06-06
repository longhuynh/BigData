package com.hadoop.mappers;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.Mapper;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reporter;
//
//public class WordCountMapper extends MapReduceBase 
//		implements Mapper<LongWritable, Text, Text, IntWritable>{
//	private final static IntWritable one = new IntWritable(1);
//	private Text word = new Text();
//	private long numRecords = 0;
//	@Override
//	public void map(LongWritable key, Text value,
//			OutputCollector<Text, IntWritable> output, Reporter r)
//			throws IOException {
//		String s = value.toString();
//		for (String word : s.split(" ")) {
//			if (word.length() > 0) {
//				output.collect(new Text(word), one);
//			}
//		}
//		
//	}
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
		String line = lineText.toString();
		Text currentWord = new Text();
		for (String word : WORD_BOUNDARY.split(line)) {
			if (word.isEmpty()) {
				continue;
			}
			currentWord = new Text(word);
			context.write(currentWord, one);
		}
	}
}
