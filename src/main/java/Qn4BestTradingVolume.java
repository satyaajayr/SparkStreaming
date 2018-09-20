import java.security.Timestamp;
import java.util.Comparator;
import java.util.Date;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;

public class Qn4BestTradingVolume {

	public static void main(String[] args) throws InterruptedException {
		System.setProperty("hadoop.home.dir", "C:\\Users\\Ajay\\eclipse-workspace\\SparkProgramming\\lib\\winutil");
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkApplication2");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.minutes(1));
		Logger.getRootLogger().setLevel(Level.ERROR);

		JavaDStream<String> lines = jssc.textFileStream("C:\\Users\\Ajay\\Documents\\Upgrad\\Project1\\TestStockFile");

		JavaDStream<JSONObject> jsonElements = lines.flatMap(line -> {
			JSONParser parser = new JSONParser();
			JSONArray jsonarray = (JSONArray) parser.parse(line);
			return jsonarray.iterator();
		});

		JavaPairDStream<String, Long> stockdata = jsonElements.mapToPair(line -> {
			String stockname = line.get("symbol").toString();
			JSONObject json = (JSONObject) line.get("priceData");
			Long volume = Long.parseLong(json.get("volume").toString());
			return new Tuple2<>(stockname, volume);
		});

		Function2<Long, Long, Long> SumFunction = (Long val1, Long val2) -> (val1 + val2);

		Function<JavaPairRDD<Long, String>, JavaPairRDD<Long, String>> TransformSortFunc = (
				JavaPairRDD<Long, String> rdd) -> rdd.sortByKey(false);

		JavaPairDStream<Long, String> sortedStream = stockdata
				.reduceByKeyAndWindow(SumFunction, Durations.minutes(10), Durations.minutes(10))
				.mapToPair(x -> x.swap())
				.transformToPair(TransformSortFunc);

		sortedStream.dstream().saveAsTextFiles("Qn4All", null);

		sortedStream.foreachRDD(rdd -> jssc.sparkContext().parallelizePairs(rdd.take(1)).saveAsTextFile("Qn4Top1" + System.currentTimeMillis()));

		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}

// JavaPairDStream<String, Long> stockreducedcount =
// stockdata.reduceByKeyAndWindow(SumFunction,
// Durations.minutes(10));
// JavaPairDStream<Long, String> swappedPair = stockreducedcount.mapToPair(x ->
// x.swap());
//
// JavaPairDStream<Long, String> sortedStream = swappedPair.transformToPair(
//
// new Function<JavaPairRDD<Long, String>, JavaPairRDD<Long, String>>() {
// @Override
// public JavaPairRDD<Long, String> call(JavaPairRDD<Long, String> jPairRDD)
// throws Exception {
// return jPairRDD.sortByKey(false);
// }
// });
//private class TupleComparator implements Comparator<Tuple2<Long, String>>, Serializable {
//@Override
//public int compare(Tuple2<Long, String> tuple1, Tuple2<Long, String> tuple2) {
//	return tuple1._1 < tuple2._1 ? 0 : 1;
//}
//}