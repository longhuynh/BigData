package com.hadoop.mappers;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hadoop.dto.Item;

public class CustomerMapper extends Mapper<LongWritable, Text, Text, Item> {
	private final static IntWritable ONE = new IntWritable(1);
	private static final Pattern ITEM_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
	private static final Pattern CUSTOMER_BOUNDARY = Pattern.compile("\\b");

	/*
	 * Let the neighborhoods of X, N(X) be set of all term after X and before
	 * the next X
	 */

	public void map(LongWritable offset, Text lineText, Context context)
			throws IOException, InterruptedException {
		String line = lineText.toString().trim();
		String[] customers = CUSTOMER_BOUNDARY.split(line);

		int customerLength = customers.length;
		int u = 0;
		Item itemH;

		for (int w = 0; w < customerLength - 1; w++) {
			if (customers[w] != null && !customers[w].isEmpty()) {
				String[] array = "|".split(customers[w]);
				String firstName = array[0];
				String[] items = CUSTOMER_BOUNDARY.split(array[1]);
				int itemLength = items.length;

				itemH = new Item();

				for (u = 0; u < itemLength; u++) {
					if (items[u] != null && !items[u].isEmpty()) {
						Text t = new Text(items[u]);
						itemH.put(t, ONE);
					}
				}

				context.write(new Text(firstName), itemH);
			}
		}
	}
}
