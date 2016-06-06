package com.myhadoop.formatoutput.reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import com.myhadoop.dto.Pair;
import com.myhadoop.dto.SortedStripe;

public class HybridCrfReducer extends Reducer<Pair, IntWritable, Text, SortedStripe> {
	private SortedStripe stripeHf;
	
	@Override
	protected void reduce(Pair pair, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		
		for (IntWritable count : counts) {
			sum += count.get();
		}
		
		// Element-wise sum: SUM(H{p.w}, new Pair(p.u, sum) 
		Text pair_W = new Text(pair.getKey());
		Text pair_U = new Text(pair.getValue());
		SortedStripe stripeH = (SortedStripe) stripeHf.get(pair_W);
		if (stripeH == null) {
			stripeH = new SortedStripe();
			stripeH.put(pair_U, new DoubleWritable(sum));
		}
		else {
//			stripeH.put(pair_U, new DoubleWritable(((DoubleWritable) stripeH.get(pair_U)).get() + sum));
			DoubleWritable tempVal = (DoubleWritable) stripeH.get(pair_U);
			if (tempVal == null) {
				stripeH.put(pair_U, new DoubleWritable(sum));
			}
			else {
				tempVal.set(tempVal.get() + sum);
			}
		}
		stripeHf.put(pair_W, stripeH);
//		context.write(pair.getKey(), stripeHf);
	}

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		stripeHf = new SortedStripe();
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		SortedStripe stripeH;
		SortedStripe newStripeH;
		Iterator<WritableComparable> it_Hf;
		Iterator<WritableComparable> it_H;
		StringBuffer sb;
		double total;
		
		it_Hf = stripeHf.keySet().iterator();
		while (it_Hf.hasNext()) {
			Text w = (Text) it_Hf.next();
			
			// calculate total
			total = 0;
			stripeH = (SortedStripe) stripeHf.get(w);
			it_H = stripeH.keySet().iterator();
			while (it_H.hasNext()) {
				Text u = (Text) it_H.next();
				
				total += ((DoubleWritable) stripeH.get(u)).get();
			}
			
			// create new H for decoration
			newStripeH = new SortedStripe();
			stripeH = (SortedStripe) stripeHf.get(w);
			it_H = stripeH.keySet().iterator();
			while (it_H.hasNext()) {
				Text u = (Text) it_H.next();
				DoubleWritable tmpVal = (DoubleWritable) stripeH.get(u);
				
				double h_u = tmpVal.get();
				sb = new StringBuffer();
				sb.append((int)h_u).append("/").append((int)total);
				newStripeH.put(u, new Text(sb.toString()));
				
				// update H{u} = H{u} / total
				tmpVal.set(tmpVal.get() / total);
			}
			// return <Text, Text>
			context.write(w, newStripeH);
			
			// if needed, we can return <Text, DoubleWritable> 
			//context.write(w, stripeH);
		}
	}
}
