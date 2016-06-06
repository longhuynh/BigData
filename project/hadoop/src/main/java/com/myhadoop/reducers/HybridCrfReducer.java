package com.myhadoop.reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com.myhadoop.dto.Pair;
import com.myhadoop.dto.Stripe;

public class HybridCrfReducer extends Reducer<Pair, IntWritable, Text, Stripe> {
	private Stripe stripeHf;
	
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
		Stripe stripeH = (Stripe) stripeHf.get(pair_W);
		if (stripeH == null) {
			stripeH = new Stripe();
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
		stripeHf = new Stripe();
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		Stripe stripeH;
		Iterator<Writable> it_Hf;
		Iterator<Writable> it_H;
		double total;
		
		it_Hf = stripeHf.keySet().iterator();
		while (it_Hf.hasNext()) {
			Text w = (Text) it_Hf.next();
			
			// calculate total
			total = 0;
			stripeH = (Stripe) stripeHf.get(w);
			it_H = stripeH.keySet().iterator();
			while (it_H.hasNext()) {
				Text u = (Text) it_H.next();
				
				total += ((DoubleWritable) stripeH.get(u)).get();
			}
			
			stripeH = (Stripe) stripeHf.get(w);
			it_H = stripeH.keySet().iterator();
			while (it_H.hasNext()) {
				Text u = (Text) it_H.next();
				DoubleWritable tmpVal = (DoubleWritable) stripeH.get(u);
				
				// update H{u} = H{u} / total
				tmpVal.set(tmpVal.get() / total);
			}
			// return <Text, DoubleWritable> 
			context.write(w, stripeH);
		}
	}
}
