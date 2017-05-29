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
class Adlog implements  java.io.Serializable
{
    private int adLogType;

    public int getAdLogType() {
        return adLogType;
    }

    public void setAdLogType(int adLogType) {
        this.adLogType = adLogType;
    }
}

public class Main {
    public static void main(String [] args)
    {
        SparkConf conf = new SparkConf().setAppName("ConversionA").setMaster(args[0]);
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));
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
        JavaPairDStream<String, Integer> impStream = adlogJavaPairDStream.filter(x->(x._2().getAdLogType() ==1)).mapToPair(x->new Tuple2<String,Integer>(x._1(),1));
        JavaPairDStream<String, Integer> clickStream = adlogJavaPairDStream.filter(x->(x._2().getAdLogType() ==2)).mapToPair(x->new Tuple2<String,Integer>(x._1(),1));
        JavaPairDStream<String, Integer> trackerStream = conversionDStream.mapToPair(x->new Tuple2<String,Integer>(x._1(),2));
        impStream = impStream.union(trackerStream);
        clickStream = clickStream.union(trackerStream);

        Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc =
                (word, one, state) -> {
                    Integer current = one.orElse(0);
                    int result = 0;
                    if (state.exists() )
                    {
                        if (current == 2 && state.get()==1)
                                result  =3;
                        if (current == 1 && state.get()==2)
                                result  =3;
                        if (current == 1 && state.get()==1)
                                result  =1;
                        if (current == 2 && state.get()==2)
                                result  =2;
                    }
		    else
		    {
                     	result = current;
		    }
		    if (result ==3) System.out.println("HERE" + word);
                    Tuple2<String, Integer> output = new Tuple2<>(word, result);
                    if (!state.isTimingOut()) state.update(result);
                    return output;
                };
        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream =
            impStream.mapWithState(StateSpec.function(mappingFunc).timeout(new Duration(60000*60*24*2)));
        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> clickStateDstream =
                clickStream.mapWithState(StateSpec.function(mappingFunc).timeout(new Duration(60000*60*24*30)));
        stateDstream.print(2);
        clickStateDstream.print(2);
        JavaPairDStream<String,Integer> finalStream = stateDstream.stateSnapshots().filter(x-> (x._2()==3));
        finalStream.print(2);
        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Hello Conversion");
    }
}
