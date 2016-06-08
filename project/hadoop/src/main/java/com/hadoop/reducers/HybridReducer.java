package com.hadoop.reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com.hadoop.dto.Pair;
import com.hadoop.dto.Stripe;

public class HybridReducer extends Reducer<Pair, IntWritable, Text, Stripe> {
	private Stripe stripeHf;
	
	@Override
	protected void reduce(Pair pair, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		
		for (IntWritable count : counts) {
			sum += count.get();
		}
		
		// Element-wise sum: SUM(H{p.w}, new Pair(p.u, sum) 
		Text pairW = new Text(pair.getKey());
		Text pairU = new Text(pair.getValue());
		Stripe stripeH = (Stripe) stripeHf.get(pairW);
		if (stripeH == null) {
			stripeH = new Stripe();
			stripeH.put(pairU, new DoubleWritable(sum));
		}
		else {
			DoubleWritable tempValue = (DoubleWritable) stripeH.get(pairU);
			if (tempValue == null) {
				stripeH.put(pairU, new DoubleWritable(sum));
			}
			else {
				tempValue.set(tempValue.get() + sum);
			}
		}
		stripeHf.put(pairW, stripeH);
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
		Iterator<Writable> iteratorHf;
		Iterator<Writable> iteratorH;
		double marginal;
		
		iteratorHf = stripeHf.keySet().iterator();
		while (iteratorHf.hasNext()) {
			Text w = (Text) iteratorHf.next();
			
			marginal = 0;
			stripeH = (Stripe) stripeHf.get(w);
			iteratorH = stripeH.keySet().iterator();
			while (iteratorH.hasNext()) {
				Text u = (Text) iteratorH.next();
				
				marginal += ((DoubleWritable) stripeH.get(u)).get();
			}
			
			stripeH = (Stripe) stripeHf.get(w);
			iteratorH = stripeH.keySet().iterator();
			while (iteratorH.hasNext()) {
				Text u = (Text) iteratorH.next();
				DoubleWritable tmpVal = (DoubleWritable) stripeH.get(u);
				
				// update H{u} = H{u} / marginal
				tmpVal.set(tmpVal.get() / marginal);
			}
			// return <Text, DoubleWritable> 
			context.write(w, stripeH);
		}
	}
}
