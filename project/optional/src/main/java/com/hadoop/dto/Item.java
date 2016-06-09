package com.hadoop.dto;

import java.util.Iterator;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Item extends MapWritable {

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("[ ");
		Iterator<Writable> it = keySet().iterator();
		while (it.hasNext()) {
			Text t = (Text) it.next();
			sb.append(t.toString()).append(", ");
		}
		sb.setLength(sb.length() - 2);
		sb.append(" ]");

		return sb.toString();
	}

}
