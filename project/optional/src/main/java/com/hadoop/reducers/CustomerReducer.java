package com.hadoop.reducers;

import com.hadoop.dto.SortedItem;
import com.hadoop.dto.Item;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

public class CustomerReducer extends Reducer<Text, Item, Text, SortedItem> {
	private final static DoubleWritable ZERO = new DoubleWritable(0);

	@SuppressWarnings("rawtypes")
	@Override
	protected void reduce(Text w, Iterable<Item> stripes, Context context)
			throws IOException, InterruptedException {
		int marginal = 0;
		int ht;
		SortedItem stripeHf = new SortedItem();
		Iterator<Writable> iterator;
		Iterator<WritableComparable> sortedIterator;
		StringBuffer sb;

		for (Item stripeH : stripes) {
			iterator = stripeH.keySet().iterator();

			while (iterator.hasNext()) {
				Text t = (Text) iterator.next();

				// H{t}
				ht = ((IntWritable) stripeH.get(t)).get();

				marginal += ht;

				// get value of Hf{t} if any, if not, create new element<Text,
				// DoubleWritable>
				DoubleWritable tempValue = (DoubleWritable) stripeHf.get(t);

				// update Hf{t} = Hf{t} + H{t}
				if (tempValue == null) {
					tempValue = ZERO;
				}
				tempValue = new DoubleWritable(tempValue.get() + (double) ht);

				stripeHf.put(t, tempValue);
			}
			// context.write(w, stripeH);
		}

		// return Text instead of DoubleWritable as above
		SortedItem newHf = new SortedItem();
		sortedIterator = stripeHf.keySet().iterator();
		while (sortedIterator.hasNext()) {
			Text t = (Text) sortedIterator.next();

			DoubleWritable tempVal = (DoubleWritable) stripeHf.get(t);
			
			double d = tempVal.get();
			sb = new StringBuffer();
			newHf.put(t, new Text(sb.append(String.valueOf((int) d))
					.append("/").append(String.valueOf(marginal)).toString()));

			// update Hf{t} = Hf{t}/total
			tempVal.set(tempVal.get() / marginal);
		}
		context.write(w, newHf);
		// if needed, you can return DoubleWritable whenever you want
		// context.write(w, stripeHf);
	}
}
