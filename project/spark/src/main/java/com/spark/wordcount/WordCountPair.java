package com.spark.wordcount;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.spark.util.Split;

import scala.Tuple2;

public class WordCountPair {
	// @SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: JavaWordCount <file>");
			System.exit(1);
		}

		String inputFile = args[0];
		String outputFile = args[1];

		SparkConf sparkConf = new SparkConf().setAppName("WordCountPair");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = context.textFile(inputFile);

		// Use Split as subclass of FlatMapFunction and pass an instance to
		// flatMap
		JavaRDD<String> words = lines.flatMap(new Split(" "));

		JavaPairRDD<String, Integer> ones = words
				.mapToPair(new PairFunction<String, String, Integer>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<String, Integer> call(String t)
							throws Exception {
						return new Tuple2<String, Integer>(t, 1);
					}
				});
		
		System.out.println("---- ONES - MAPPER OUTPUT ----");
		List<Tuple2<String, Integer>> outMapper = ones.collect();
		for (Tuple2<?, ?> tuple : outMapper) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}

		JavaPairRDD<String, Integer> counts = ones
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Integer call(Integer x, Integer y) throws Exception {
						return x + y;
					}
				});
		
		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}
		
		counts.saveAsTextFile(outputFile);

		context.stop();
		context.close();
	}
}
