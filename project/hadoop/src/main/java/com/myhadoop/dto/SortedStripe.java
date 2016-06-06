package com.myhadoop.dto;

import java.util.Iterator;

import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class SortedStripe extends SortedMapWritable {

	@SuppressWarnings("rawtypes")
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("[ ");
		Iterator<WritableComparable> it = keySet().iterator();
		while(it.hasNext()) {
			Text t = (Text) it.next();
			String val = get(t).toString();
			sb.append("(").append(t.toString()).append(", ").append(val).append("), ");
		}
		sb.setLength(sb.length() - 2);
		sb.append(" ]");
		
		return sb.toString();
	}

}
