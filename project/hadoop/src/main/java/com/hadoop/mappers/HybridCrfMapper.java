package com.hadoop.mappers;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.log4j.Logger;


import com.hadoop.dto.Pair;

public class HybridCrfMapper extends Mapper<LongWritable, Text, Pair, IntWritable> {
//	private static final Logger LOG = Logger.getLogger(HybridCrfMapper.class);
	private final static IntWritable ONE = new IntWritable(1);
	private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
	/*
	 * Let the neighborhoods of X, N(X) be set of all term after X and before the next X
	 */
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	public void map(LongWritable offset, Text lineText, Context context) 
			throws IOException, InterruptedException {
		String line = lineText.toString().trim();
		String[] arr = WORD_BOUNDARY.split(line);
		
		int len = arr.length;
		int i = 0;
		int j = 0;
		for (; i < len - 1; i++) {
			if (arr[i] != null && !arr[i].isEmpty()) {
				for (j = i + 1; j < len; j++) {
					if (arr[j] != null && !arr[j].isEmpty()) {
						if (!arr[i].equals(arr[j])) {
							context.write(new Pair(arr[i], arr[j]), ONE);
							//LOG.debug(new Pair(new Text(arr[i]), new Text(arr[j])).toString() + ", 1.");
						}
						else {
							break;
						}
					}
				}
			}
		}
	}
}
