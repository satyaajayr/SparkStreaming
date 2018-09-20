import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import scala.Tuple2;

public class Qn3RelativeStrengthIndex {

    public static void main(String[] args) throws InterruptedException {
    	System.setProperty("hadoop.home.dir", "C:\\Users\\Ajay\\eclipse-workspace\\SparkProgramming\\lib\\winutil");
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkApplication3");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.minutes(1));
        jssc.checkpoint("checkpoint_dir");
        Logger.getRootLogger().setLevel(Level.ERROR);

        JavaDStream<String> lines = jssc.textFileStream("C:\\Users\\Ajay\\Documents\\Upgrad\\Project1\\TestStockFile");

        JavaDStream<JSONObject> jsonElements = lines.flatMap(line -> {
            JSONParser parser = new JSONParser();
            JSONArray jsonarray = (JSONArray) parser.parse(line);
            return jsonarray.iterator();
        });
        
        jsonElements.print();

        JavaPairDStream<String, Tuple2<Double, Double>> stockdata = jsonElements.mapToPair(line -> {
            String stockname = line.get("symbol").toString();
            JSONObject json = (JSONObject) line.get("priceData");
            Double closeprice = Double.parseDouble(json.get("close").toString());
            Double openprice = Double.parseDouble(json.get("open").toString());

            Double gain = 0.0;
            Double loss = 0.0;

            if (closeprice > openprice) {
                gain = closeprice - openprice;
            } else if (openprice > closeprice) {
                loss = openprice - closeprice;
            }

            return new Tuple2<>(stockname, new Tuple2<>(gain, loss));
        });
        stockdata.print();

        JavaPairDStream<String, Double[]> stockavgstats = stockdata.updateStateByKey((values, state) -> {
            Double[] prevstate = state.or(new Double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0});

            double counter = prevstate[0];
            double totgain = prevstate[1];
            double totloss = prevstate[2];
            double avggain = prevstate[3];
            double avgloss = prevstate[4];
            double rs = prevstate[5];
            double rsi = prevstate[6];

            for (Tuple2<Double, Double> tuple : values) {

                counter += 1;
                
                if(counter >= 1){
                totgain += tuple._1();
                totloss += tuple._2();
                }

                if (counter >= 1) {
                    if (avggain == 0.0 && avgloss == 0.0) {
                        avggain = totgain / counter;
                        avgloss = totloss / counter;
                    } else {
                        avggain = ((avggain * (counter - 1)) + tuple._1()) / counter;
                        avgloss = ((avgloss * (counter - 1)) + tuple._2()) / counter;
                    }

                    if (avggain == 0.0) {
                        rsi = 100;
                    } else if (avgloss == 0.0) {
                        rsi = 0;
                    } else {
                        rs = avggain / avgloss;
                        rsi = 100 - (100 / (1 + rs));
                    }
                }

            }
            System.out.println(counter + "|" + totgain + "|" + totloss + "|" + avggain + "|" + avgloss);
            return Optional.of(new Double[]{counter, totgain, totloss, avggain, avgloss, rs, rsi});
        });

        JavaPairDStream<String, Double> stocksrsi = stockavgstats.mapValues(x -> x[6]);

        stocksrsi.window(Durations.minutes(10), Durations.minutes(5)).dstream().saveAsTextFiles("Qn3", null);

        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }

}

