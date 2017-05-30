import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by anshuman on 30/5/17.
 */
public class ConversionKafkaProducer  implements java.io.Serializable{
     transient private Producer<String, String> producer;
    
    Producer<String,String> getProducer()
    {
	if (producer ==null)
	{
       	Properties props = new Properties();
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("producer.type", "async");
        props.put("metadata.broker.list", "192.169.34.61:9092,192.169.34.62:9092,192.169.34.63:9092");
        props.put("bootstrap.servers", "192.169.34.61:9092,192.169.34.62:9092,192.169.34.63:9092");
        producer = new KafkaProducer<String, String>(props);

	}
	return producer;
    }

    void send(String key, String value) {
        getProducer().send(new ProducerRecord<String, String>("AdConverted",key, key+":" + value));
    }
}
