import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;



/**
 * Created by anshuman on 3/5/17.
 */


public class Main {
    public static void main(String [] args)
    {
        SparkConf conf = new SparkConf().setAppName("Conversion");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(5*60000));
	ssc.checkpoint("hdfs://172.29.65.171:8020/user/Conversion");
        KafkaConnector kafkaConnector = new KafkaConnector();
        JavaPairDStream<String,String> javaPairDStream =  kafkaConnector.getStream(ssc);
        JavaPairDStream<String, Integer> impressionPD = javaPairDStream.mapToPair(x-> new Tuple2<String, Integer>(x._1(),1));

        Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc =
                (word, one, state) -> {
                    int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
                    Tuple2<String, Integer> output = new Tuple2<>(word, sum);
                    if (!state.isTimingOut()) state.update(sum);
                    return output;
                };
        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream =
        impressionPD.mapWithState(StateSpec.function(mappingFunc).timeout(new Duration(60000*60*6)));
        stateDstream.print();
        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Hello Conversion");
    }
}
