import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.log4j.*;

public class FirstSparkApplication {

	public static void main(String[] args) throws InterruptedException {
		System.setProperty("hadoop.home.dir", "C:\\Users\\Ajay\\eclipse-workspace\\SparkProgramming\\lib\\winutil");
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("FirstSparkApplication");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60));
		Logger.getRootLogger().setLevel(Level.ERROR);
		JavaDStream<String> lines = jssc.textFileStream("C:\\Users\\Ajay\\Documents\\Upgrad\\Project1\\TestStockFile");
		lines.print();
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}

}
