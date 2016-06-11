package com.hadoop.reducers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com.hadoop.dto.Item;

public class CustomerReducer extends Reducer<Text, Item, Text, Item> {
	private final static IntWritable ONE = new IntWritable(1);
	
	@Override
	protected void reduce(Text w, Iterable<Item> items, Context context)
			throws IOException, InterruptedException {

		Iterator<Writable> iterator;
		
		List<Text> intersectionList = new ArrayList<Text>();	
		
		for (Item itemH : items) {
			iterator = itemH.keySet().iterator();		
			List<Text> texts = new ArrayList<Text>();	
			
			while (iterator.hasNext()) {
				Text t = (Text) iterator.next();
				texts.add(t);				
			}
			
			intersectionList = intersection(texts, intersectionList);
		}

		Item newH = new Item();
		
		for(Text t : intersectionList){
			newH.put(t,ONE);
		}		
		context.write(w, newH);
	}

	public <T> List<T> intersection(List<T> list1, List<T> list2) {
		if(list2.size() == 0)
			return list1;
		
		List<T> list = new ArrayList<T>();

		for (T t : list1) {
			if (list2.contains(t)) {
				list.add(t);
			}
		}

		return list;
	}
}
