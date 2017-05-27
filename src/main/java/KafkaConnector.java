import kafka.serializer.StringDecoder;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by anshuman on 3/5/17.
 */
public class KafkaConnector {
    public KafkaConnector() {}


    public JavaPairDStream<String, String>  getStream(JavaStreamingContext ssc)
    {
        Map<String,String>  kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", "172.29.65.180:9092,172.29.65.181:9092," +
                "172.29.65.182:9092,172.29.65.183:9092,172.29.65.184:9092,172.29.65.185:9092," +
                "172.29.65.186:9092,172.29.65.187:9092,172.29.65.188:9092,172.29.65.189:9092");
        Set<String> topics = new HashSet<String>();
        topics.add("AdServe");
        return KafkaUtils.createDirectStream(ssc, String.class, String.class,
                StringDecoder.class, StringDecoder.class ,kafkaParams,topics);
    }
}
