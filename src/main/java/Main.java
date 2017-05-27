import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


/**
 * Created by anshuman on 3/5/17.
 */


public class Main {
    public static void main(String [] args)
    {
        SparkConf conf = new SparkConf().setAppName("Conversion").setMaster("local[4]");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));
        KafkaConnector kafkaConnector = new KafkaConnector();
        JavaPairDStream<String,String> javaPairDStream =  kafkaConnector.getStream(ssc);
        javaPairDStream.print(2);
        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Hello Conversion");
    }
}
