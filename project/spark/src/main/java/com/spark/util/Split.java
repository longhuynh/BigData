package com.spark.util;

import org.apache.spark.api.java.function.FlatMapFunction;
import java.util.Arrays;

public class Split implements FlatMapFunction<String, String> {
	private static final long serialVersionUID = 1L;
	private String pattern;
	
	public Split(String pattern) {
		this.pattern = pattern;
	}

	public Iterable<String> call(String arg0) throws Exception {
		return Arrays.asList(arg0.split(pattern));
	}
}