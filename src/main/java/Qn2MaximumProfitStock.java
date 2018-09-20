import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import scala.Tuple2;
import scala.Tuple3;

public class Qn2MaximumProfitStock {

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

        JavaPairDStream<String,Tuple2<Double,Double>> stockdata = jsonElements.mapToPair(line -> {
            String stockname = line.get("symbol").toString();
            JSONObject json = (JSONObject) line.get("priceData");
            String closeprice = json.get("close").toString();
            String openprice = json.get("open").toString();
            return new Tuple2<>(stockname, new Tuple2<>(Double.parseDouble(closeprice),Double.parseDouble(openprice)));
        });
        
        //JavaPairDStream<String, Tuple3<Double,Double,Integer>> stockdatacount = stockdata.mapValues(tuple -> new Tuple3<>(tuple._1,tuple._2,1));
        //JavaPairDStream<String, Tuple3<Double,Double,Integer>> stockreducedcount = stockdatacount.reduceByKeyAndWindow(aggregatefunction, Durations.minutes(10), Durations.minutes(5));
        //stockreducedcount.print();
        //JavaPairDStream<String, Double> stockprofitstats = stockreducedcount.mapValues(tuple -> (tuple._1() /tuple._3() - tuple._2() /tuple._3()));
        
        Function2<Tuple3<Double,Double,Integer>,Tuple3<Double,Double,Integer>,Tuple3<Double,Double,Integer>> 
        SumFunction = (Tuple3<Double,Double,Integer> First, Tuple3<Double,Double,Integer> Second) 
                        -> (new Tuple3<>(First._1()+ Second._1(), First._2() + Second._2(),First._3() + Second._3()));
        
        JavaPairDStream<String, Double> stockprofitstats = stockdata.mapValues(Value -> new Tuple3<>(Value._1,Value._2,1))
        		.reduceByKeyAndWindow(SumFunction, Durations.minutes(10), Durations.minutes(5))
        		.mapValues(Value -> (Value._1() /Value._3() - Value._2() /Value._3()));
        
        stockprofitstats.dstream().saveAsTextFiles("Qn2", null);
        
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
