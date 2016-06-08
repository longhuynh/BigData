package com.hadoop.reducers;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.hadoop.dto.Pair;

public class PairReducer extends Reducer<Pair, IntWritable, Pair, DoubleWritable> {
	private static final String STAR_SYMBOL = "*";
	private int marginal;
	
	@Override
	protected void reduce(Pair pair, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		
		for (IntWritable count : counts) {
			sum += count.get();
		}
		if (STAR_SYMBOL.equals(pair.getValue().toString())) {
			marginal = sum;
		}
		else {
			double d = new Double(sum)/marginal;
			context.write(pair, new DoubleWritable(d));
		}
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		marginal = 0;
	}
}
