package com.myspark.wordcount;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import com.myspark.util.Split;

import scala.Tuple2;

public class WordCountPair {
	private static PairFunction<String, String, Integer> pairFunc = s -> {
		return new Tuple2<String, Integer>(s, 1);
	};
	
	private static Function2<Integer, Integer, Integer> func2 = (x, y) -> x + y;

	// private static final Pattern SPACE = Pattern.compile(" ");

	//@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: JavaWordCount <file>");
			System.exit(1);
		}

		String inputFile = args[0];
		String outputFile = args[1];

		SparkConf sparkConf = new SparkConf().setAppName("WordCountPair");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(inputFile);

		// Use Split as subclass of FlatMapFunction and pass an instance to
		// flatMap
		JavaRDD<String> words = lines.flatMap(new Split(" "));

		// Map each word to a (word, 1) pair
		// Map was passed a PairFunction<String, String, Integer> and returned a
		// JavaPairRDD<String, Integer>
		// JavaPairRDD<String, Integer> ones = words.mapToPair(new
		// PairFunction<String, String, Integer>() {
		// @Override
		// public Tuple2<String, Integer> call(String s) {
		// return new Tuple2<>(s, 1);
		// }
		// });
		JavaPairRDD<String, Integer> ones = words.mapToPair(pairFunc);
		System.out.println("---- ONES - MAPPER OUTPUT ----");
		List<Tuple2<String, Integer>> outMapper = ones.collect();
		for (Tuple2<?, ?> tuple : outMapper) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}

		// Use reduceByKey to count the occurrences of each word
		// reduceByKey is passed a Function2, which implements a function with
		// two arguments. The resulting JavaPairRDD contains (word, count)
		// pairs.
		// JavaPairRDD<String, Integer> counts = ones
		// .reduceByKey(new Function2<Integer, Integer, Integer>() {
		// @Override
		// public Integer call(Integer i1, Integer i2) {
		// return i1 + i2;
		// }
		// });
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(func2);
		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}
		counts.saveAsTextFile(outputFile);

		ctx.stop();
		ctx.close();
	}

}
