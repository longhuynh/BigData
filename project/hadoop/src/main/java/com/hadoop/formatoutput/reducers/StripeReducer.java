package com.hadoop.formatoutput.reducers;

import com.hadoop.dto.SortedStripe;
import com.hadoop.dto.Stripe;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripeReducer extends Reducer<Text, Stripe, Text, SortedStripe> {
	private final static DoubleWritable ZERO = new DoubleWritable(0);
	
	@SuppressWarnings("rawtypes")
	@Override
	protected void reduce(Text w, Iterable<Stripe> stripes, Context context)
			throws IOException, InterruptedException {
		int total = 0;
		int h_t;
		SortedStripe stripeHf = new SortedStripe();
		Iterator<Writable> iterator;
		Iterator<WritableComparable> sortedIterator;
		StringBuffer sb;
		
		for (Stripe stripeH : stripes) {
			iterator = stripeH.keySet().iterator();
			
			// go through H and update Hf{t} if any
			while (iterator.hasNext()) {
				Text t = (Text) iterator.next();
				
				// H{t}
				h_t = ((IntWritable) stripeH.get(t)).get();
				
				// increase the total
				total += h_t;
				
				// get value of Hf{t} if any, if not, create new element<Text, DoubleWritable>
				DoubleWritable tempVal = (DoubleWritable) stripeHf.get(t);
				
				// update Hf{t} = Hf{t} + H{t}
				if (tempVal == null) {
					tempVal = ZERO;
				}
				tempVal = new DoubleWritable(tempVal.get() + (double)h_t);

				stripeHf.put(t, tempVal);
			}
			//context.write(w, stripeH);
		}
		
		// return Text instead of DoubleWritable as above
		SortedStripe newHf = new SortedStripe();
		sortedIterator = stripeHf.keySet().iterator();
		while (sortedIterator.hasNext()) {
			Text t = (Text) sortedIterator.next();
			
			DoubleWritable tempVal = (DoubleWritable) stripeHf.get(t);
			
			// return new Strip<Text, Text> instead of Stripe<Text, DoubleWritable>
			double d = tempVal.get();
			sb = new StringBuffer();
			newHf.put(t, new Text(
					sb.append(String.valueOf((int) d)).append("/").append(String.valueOf(total)).toString()));
			
			// update Hf{t} = Hf{t}/total
			tempVal.set(tempVal.get()/total);
		}
		context.write(w, newHf);
		// if needed, you can return DoubleWritable whenever you want
		//context.write(w, stripeHf);
		
	}

}
