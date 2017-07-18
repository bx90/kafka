import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by Boxiong on 7/17/17.
 */

/**
 * Please check the documentation in https://kafka.apache.org/documentation/
 */
public class KafkaProducer {
    public static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);


    public static void main(String[] args) {
        logger.info("Setting up properties");
        Properties properties = new Properties();
        // kafka bootstrap server
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        // producer acks
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "3");
        properties.setProperty("linger.ms", "1");

        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);
        logger.info("Producer has been created successfully. Visit http://127.0.0.1:3030/kafka-topics-ui to view the status.");


        for (int key=0; key < 10; key++){
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("my_topic", Integer.toString(key), "message #: " + Integer.toString(key));
            producer.send(producerRecord);
        }
        logger.info("Message has been sent successfully. Visit http://127.0.0.1:3030/kafka-topics-ui/#/cluster/fast-data-dev/topic/n/my_topic/ to view the data.");
        producer.flush();
        producer.close();
    }
}
