package com.myhadoop.dto;

import java.util.Iterator;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Stripe extends MapWritable {

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("[ ");
		Iterator<Writable> it = keySet().iterator();
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
