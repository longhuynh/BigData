package com.hadoop.mappers;

import com.hadoop.dto.Stripe;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.log4j.Logger;

public class StripeMapper extends Mapper<LongWritable, Text, Text, Stripe> {
//	private static final Logger LOG = Logger.getLogger(StripeCrfMapper.class);
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
		int w = 0;
		int u = 0;
		Stripe stripeH;
		
		for (; w < len - 1; w++) {
			if (arr[w] != null && !arr[w].isEmpty()) {
				stripeH = new Stripe();
				
				for (u = w + 1; u < len; u++) {
					if (arr[u] != null && !arr[u].isEmpty()) {
						if (!arr[w].equals(arr[u])) {
							Text t = new Text(arr[u]);
							IntWritable tempInt = (IntWritable) stripeH.get(t);
							
							if (tempInt == null) {
								tempInt = ONE;
							}
							else {
								tempInt = new IntWritable(tempInt.get() + 1);
							}
							
							stripeH.put(t, tempInt);
						}
						else {
							break;
						}
					}
				}
				
				context.write(new Text(arr[w]), stripeH);
			}
		}
	}
}
