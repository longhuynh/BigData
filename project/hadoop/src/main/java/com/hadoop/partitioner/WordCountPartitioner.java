package com.hadoop.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import com.hadoop.dto.Pair;

public class WordCountPartitioner extends Partitioner<Pair, IntWritable> {

	@Override
	public int getPartition(Pair pair, IntWritable p1, int numReduceTasks) {
		String chars = "abcdefghij";
		boolean result = false;
		for (int i = 0; i < chars.length(); i++) {
			if (pair != null
					&& pair.toString().toLowerCase().charAt(0) == chars
							.charAt(i)) {
				result = true;
				break;
			}
		}
		return result == true ? 0 : 1;
	}
}
