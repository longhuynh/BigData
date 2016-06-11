package com.hadoop.mappers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hadoop.dto.Item;

public class CustomerMapper extends Mapper<LongWritable, Text, Text, Item> {
	private final static IntWritable ONE = new IntWritable(1);
	private static final Pattern CUSTOMER_BOUNDARY = Pattern.compile("\\r?\\n");

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

		for (int w = 0; w < customerLength; w++) {
			if (customers[w] != null && !customers[w].isEmpty()) {
				String[] array = customers[w].split("~");
				if (array.length > 1) {
					String firstName = array[0];
					String[] items = array[1].split(" ");
					int itemLength = items.length;
					itemH = new Item();

					List<String> texts = new ArrayList<String>();

					for (u = 0; u < itemLength; u++) {
						if (items[u] != null && !items[u].isEmpty()) {
							if (!texts.contains(items[u])) {
								texts.add(items[u]);
								Text t = new Text(items[u]);
								itemH.put(t, ONE);
							}
						}
					}

					context.write(new Text(firstName), itemH);
				}
			}
		}
	}
}
