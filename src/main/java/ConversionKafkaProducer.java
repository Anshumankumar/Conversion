import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by anshuman on 30/5/17.
 */
public class ConversionKafkaProducer {
    Producer<String, String> producer;

    ConversionKafkaProducer() {
        Properties props = new Properties();
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("producer.type", "async");
        props.put("metadata.broker.list", "192.169.34.61:9092,192.169.34.62:9092,192.169.34.63:9092");
        producer = new KafkaProducer<String, String>(props);
    }

    void send(String key, String value) {
        producer.send(new ProducerRecord<String, String>("AdConverted",key, value));
    }
}
