import org.apache.avro.file.SyncableFileOutputStream;
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

        JavaDStream<String> lines = jssc.textFileStream(args[0]);

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
        //stockdata.print();

        JavaPairDStream<String, StateObject> stockavgstats = stockdata.updateStateByKey((values, state) -> {
            //Double[] prevstate = state.or(new Double[]{0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0});
        	
        	
//            double counter = prevstate[0];
//            double totgain = prevstate[1];
//            double totloss = prevstate[2];
//            double avggain = prevstate[3];
//            double avgloss = prevstate[4];
//            double rs = prevstate[5];	
//            double rsi = prevstate[6];
        	
        	StateObject prevstate = state.or(new StateObject(0, new Double[14], new Double[14], 0, 0, 0, 0));
//        			counter = 0, 
//                	totgain = new Double[14], 
//                	totloss = new Double[14], 
//                	avggain = 0,
//                	avgloss = 0,
//                	rs = 0,
//                	rsi = 0));
        	
        	int counter = prevstate.counter;
        	Double[] totgain = prevstate.totgain == null ? new Double[14] : prevstate.totgain;
        	Double[] totloss = prevstate.totloss == null ? new Double[14] : prevstate.totloss;
        	double avggain = prevstate.avggain;
        	double avgloss = prevstate.avgloss;
        	
        	double rs = prevstate.rs;
        	double rsi = prevstate.rsi;
        	
        	System.out.println("counter" + counter);
        	System.out.print("totgain : ");
        	for(int j = 0; j < totgain.length; j++)
        	{
        		if(totgain[j] == null)
        		{
        			totgain[j] = 0.0;
        		}
        		System.out.print(totgain[j] + ", ");
        	}
        	System.out.println("");
        	System.out.print("totloss : ");
        	for(int j = 0; j < totloss.length; j++)
        	{
        		if(totloss[j] == null)
        		{
        			totloss[j] = 0.0;
        		}
        		System.out.print(totloss[j] + ", ");
        	}
        	System.out.println("");
        	System.out.println("avggain" + avggain);
        	System.out.println("avgloss" + avgloss);
        	System.out.println("rs" + rs);
        	System.out.println("rsi" + rsi);

            for (Tuple2<Double, Double> tuple : values) {

                counter += 1;
                double tempavggain = 0;
            	double tempavgloss = 0;

                if(counter >= 1){
                totgain[(counter - 1) % 14] = tuple._1();
                totloss[(counter - 1) % 14] = tuple._2();
                }
                System.out.print("inside totgain : ");
            	for(int j = 0; j < totgain.length; j++)
            	{
            		System.out.print(totgain[j] + ", ");
            	}
            	System.out.println("");
            	System.out.print("inside totloss : ");
            	for(int j = 0; j < totloss.length; j++)
            	{
            		System.out.print(totloss[j] + ", ");
            	}
                System.out.println("counter: " + counter + "totgain.length: " + totgain.length );
                System.out.println("tuples: " + tuple._1() + ", " + tuple._2());
                for(int j = 0; j < totgain.length; j++) {
                	tempavggain += totgain[j];
                	tempavgloss += totloss[j];
                }
                tempavggain -= totgain[(counter - 1) % 14];
                tempavgloss -= totloss[(counter - 1) % 14];

                if (counter == 1) {
                    if (avggain == 0.0 && avgloss == 0.0) {
                        avggain = totgain[0] / counter;
                        avgloss = totloss[0] / counter;
                    } else if(counter > 1 && counter <= 14) {
                        avggain = ((avggain * (counter - 1)) + tuple._1()) / counter;
                        avgloss = ((avgloss * (counter - 1)) + tuple._2()) / counter;
                    }
                    else {
                    	avggain = ((tempavggain * 13) + tuple._1()) / 14;
                        avgloss = ((tempavgloss * 13) + tuple._2()) / 14;
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
            System.out.println(counter + "|" + rs + "|" + rsi + "|" + avggain + "|" + avgloss);
            //return Optional.of(new Double[]{counter, totgain, totloss, avggain, avgloss, rs, rsi});
            return Optional.of(new StateObject(
            	counter, 
            	totgain, 
            	totloss, 
            	avggain,
            	avgloss,
            	rs,
            	rsi
            ));
        });

        JavaPairDStream<String, Double> stocksrsi = stockavgstats.mapValues(x -> x.rsi);

        stocksrsi.window(Durations.minutes(10), Durations.minutes(1)).dstream().saveAsTextFiles(args[1], null);

        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }

}

