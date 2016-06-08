package com.hadoop.formatoutput.reducers;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import com.hadoop.dto.Pair;
import com.hadoop.dto.SortedStripe;

public class HybridReducer extends Reducer<Pair, IntWritable, Text, SortedStripe> {
	private SortedStripe stripeHf;
	
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
		SortedStripe stripeH = (SortedStripe) stripeHf.get(pairW);
		if (stripeH == null) {
			stripeH = new SortedStripe();
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
		stripeHf = new SortedStripe();
	}

	@SuppressWarnings("rawtypes")
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		SortedStripe stripeH;
		SortedStripe newStripeH;
		Iterator<WritableComparable> iteratorHf;
		Iterator<WritableComparable> iteratorH;
		StringBuffer sb;
		double marginal;
		
		iteratorHf = stripeHf.keySet().iterator();
		while (iteratorHf.hasNext()) {
			Text w = (Text) iteratorHf.next();
			
			marginal = 0;
			stripeH = (SortedStripe) stripeHf.get(w);
			iteratorH = stripeH.keySet().iterator();
			while (iteratorH.hasNext()) {
				Text u = (Text) iteratorH.next();
				
				marginal += ((DoubleWritable) stripeH.get(u)).get();
			}
			
			// create new H for decoration
			newStripeH = new SortedStripe();
			stripeH = (SortedStripe) stripeHf.get(w);
			iteratorH = stripeH.keySet().iterator();
			while (iteratorH.hasNext()) {
				Text u = (Text) iteratorH.next();
				DoubleWritable tmpVal = (DoubleWritable) stripeH.get(u);
				
				double hu = tmpVal.get();
				sb = new StringBuffer();
				sb.append((int)hu).append("/").append((int)marginal);
				newStripeH.put(u, new Text(sb.toString()));
				
				// update H{u} = H{u} / marginal
				tmpVal.set(tmpVal.get() / marginal);
			}
			// return <Text, Text>
			context.write(w, newStripeH);
		}
	}
}
