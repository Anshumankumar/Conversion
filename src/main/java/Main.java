import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
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

@JsonIgnoreProperties(ignoreUnknown = true)
class Impression implements java.io.Serializable {
    private String imprId;

    public String getImprId() {
        return imprId;
    }

    public void setImprId(String imprId) {
        this.imprId = imprId;
    }
}
@JsonIgnoreProperties(ignoreUnknown = true)
class Adlog implements  java.io.Serializable {
    private int adLogType;
    private Impression adImprLog;
    private Impression adClickLog;

    public int getAdLogType() {
        return adLogType;
    }

    public void setAdLogType(int adLogType) {
        this.adLogType = adLogType;
    }

    public Impression getAdImprLog() {
        return adImprLog;
    }

    public void setAdImprLog(Impression adImprLog) {
        this.adImprLog = adImprLog;
    }

    public Impression getAdClickLog() {
        return adClickLog;
    }

    public void setAdClickLog(Impression adClickLog) {
        this.adClickLog = adClickLog;
    }

    public String toString() {
        if (adLogType == 1)
            return getAdLogType() + ":" +getAdImprLog().getImprId();
        if (adLogType == 2)
            return getAdLogType() + ":" +getAdClickLog().getImprId();
        return null;
    }
}

public class Main {
    public static void main(String [] args)
    {
        SparkConf conf = new SparkConf().setAppName("Conversion").setMaster(args[0]);
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(60000));
        ssc.checkpoint("hdfs://172.29.65.171:8020/user/Conversion");
        KafkaConnector kafkaConnector = new KafkaConnector();
        JavaPairDStream<String,String> javaPairDStream =  kafkaConnector.getStream(ssc,"AdServe");
        JavaPairDStream<String,String> conversionDStream =  kafkaConnector.getStream(ssc,"AdTracker");

        ObjectMapper mapper = new ObjectMapper();
        JavaPairDStream<String, Adlog> adlogJavaPairDStream = javaPairDStream.mapToPair(x->
        {
            Adlog adlog = mapper.readValue(x._2(),Adlog.class);
            return new Tuple2<String, Adlog>(x._1(),adlog);
        });
        javaPairDStream.print();
	    conversionDStream.print();
	    adlogJavaPairDStream.filter(x->(x._2().getAdLogType()==2)).print();
        JavaPairDStream<String, String> impStream = adlogJavaPairDStream.filter(x->(x._2().getAdLogType() ==1)).mapToPair(x->new Tuple2<String,String>(x._1(),x._2().getAdImprLog().getImprId()));
        JavaPairDStream<String, String> clickStream = adlogJavaPairDStream.filter(x->(x._2().getAdLogType() ==2)).mapToPair(x->new Tuple2<String,String>(x._1(),x._2().getAdClickLog().getImprId()));
        JavaPairDStream<String, String> trackerStream = conversionDStream.mapToPair(x->new Tuple2<String,String>(x._1(), "tracker"));
        impStream = impStream.union(trackerStream);
        clickStream = clickStream.union(trackerStream);

        ConversionKafkaProducer conversionKafkaProducer = new ConversionKafkaProducer();

        Function3<String, Optional<String>, State<String>, Tuple2<String, String>> mappingFunc =
                (key, current, state) -> {
                    String currentState =  current.orElse(null);
                    Tuple2<String, String> output = null;
                    if (currentState.equals("tracker"))
                    {
                        if (state.exists())
                        {
                            conversionKafkaProducer.send(key, state.get());
                        }
                    }
                    else
                    {
                        if (!state.isTimingOut())
                        {
                             output = new Tuple2<>(key, currentState);
                             state.update(currentState);
                        }
                    }
                    return output;
                };

        JavaMapWithStateDStream<String, String, String, Tuple2<String, String>> stateDstream =
            impStream.mapWithState(StateSpec.function(mappingFunc).timeout(new Duration(60000*60*24*2)));
        JavaMapWithStateDStream<String, String, String, Tuple2<String, String>> clickStateDstream =
                clickStream.mapWithState(StateSpec.function(mappingFunc).timeout(new Duration(60000*60*24*30)));
        stateDstream.print(2);
        clickStateDstream.print(2);
        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
