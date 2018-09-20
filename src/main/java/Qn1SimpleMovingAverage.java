
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.log4j.*;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import scala.Tuple2;

public class Qn1SimpleMovingAverage {

    public static void main(String[] args) throws InterruptedException {
    	System.setProperty("hadoop.home.dir", "C:\\Users\\Ajay\\eclipse-workspace\\SparkProgramming\\lib\\winutil");
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkApplication1");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.minutes(1));
        Logger.getRootLogger().setLevel(Level.ERROR);

        JavaDStream<String> lines = jssc.textFileStream("C:\\Users\\Ajay\\Documents\\Upgrad\\Project1\\TestStockFile");

        JavaDStream<JSONObject> jsonElements = lines.flatMap(line -> {
            JSONParser parser = new JSONParser();
            JSONArray jsonarray = (JSONArray) parser.parse(line);
            return jsonarray.iterator();
        });
        
        
        JavaPairDStream<String, Double> data = jsonElements.mapToPair(line -> {
            String stockname = line.get("symbol").toString();
            JSONObject json = (JSONObject) line.get("priceData");
            String closeprice = json.get("close").toString();
            return new Tuple2<>(stockname, Double.parseDouble(closeprice));
        });

        //JavaPairDStream<String, Tuple2<Double, Integer>> datacount = data.mapValues(val -> new Tuple2<>(val, 1));
        //JavaPairDStream<String, Tuple2<Double, Integer>> sumdata = datacount.reduceByKeyAndWindow(SumFunction, Durations.minutes(10), Durations.minutes(5));
        //stockreducedcount.print();
        //JavaPairDStream<String, Double> averagedata = sumdata.mapValues(tuple -> tuple._1() / tuple._2());

        Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>> 
                SumFunction = (Tuple2<Double, Integer> tuple1, Tuple2<Double, Integer> tuple2) 
                        -> (new Tuple2<>(tuple1._1() + tuple2._1(), tuple1._2() + tuple2._2()));

        JavaPairDStream<String, Double> averagedata = data.mapValues(val -> new Tuple2<>(val, 1)).cache()
        		.reduceByKeyAndWindow(SumFunction, Durations.minutes(10), Durations.minutes(5))
        		.mapValues(tuple -> tuple._1() / tuple._2());
        
        averagedata.dstream().saveAsTextFiles("Qn1", null);

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
