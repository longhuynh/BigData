package com.hadoop.formatoutput.reducers;

import com.hadoop.dto.Pair;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//public class PairCrfReducer extends Reducer<Pair, IntWritable, Pair, DoubleWritable> {
public class PairReducer extends Reducer<Pair, IntWritable, Pair, Text> {
	private static final String STAR_SYMBOL = "*";
	private int total;
	
	@Override
	protected void reduce(Pair pair, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		StringBuilder sb;
		
		for (IntWritable count : counts) {
			sum += count.get();
		}
		if (STAR_SYMBOL.equals(pair.getValue().toString())) {
			total = sum;
		}
		else {
//			double d = new Double(sum)/total;
//			context.write(pair, new DoubleWritable(d));
//			DecimalFormat twoDForm = new DecimalFormat("#.00");
//			twoDForm.format(d);	// it will return String
			sb = new StringBuilder();
			sb.append(sum).append("/").append(total);
			context.write(pair, new Text(sb.toString()));
		}
//		context.write(pair, new DoubleWritable(new Double(sum)));
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		total = 0;
	}
}
