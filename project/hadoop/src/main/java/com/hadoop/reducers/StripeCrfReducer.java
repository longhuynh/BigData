package com.hadoop.reducers;

import com.hadoop.dto.Stripe;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripeCrfReducer extends Reducer<Text, Stripe, Text, Stripe> {
	private final static DoubleWritable ZERO = new DoubleWritable(0);
	
	@Override
	protected void reduce(Text w, Iterable<Stripe> stripes, Context context)
			throws IOException, InterruptedException {
		int total = 0;
		int h_t;
		Stripe stripeHf = new Stripe();
		Iterator<Writable> iterator;
		
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
		
		iterator = stripeHf.keySet().iterator();
		while (iterator.hasNext()) {
			Text t = (Text) iterator.next();
			
			DoubleWritable val = (DoubleWritable) stripeHf.get(t);
			
			// update Hf{t} = Hf{t}/total
			val.set(val.get()/total);
		}
		context.write(w, stripeHf);
		
	}

}
