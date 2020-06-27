package mongodbkafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(final String[] args) {

        // consumer properties
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-group");

        // using auto commit
        props.put("enable.auto.commit", "true");

        // string inputs and outputs
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // kafka consumer object
        final KafkaConsumer<String, Object> consumer = new KafkaConsumer<String, Object>(props);

        // subscribe to topic
        consumer.subscribe(Arrays.asList("test1"));

        // infinite poll loop
        PrintStream stream = new PrintStream(System.out);
        while (true) {
            final KafkaConsumer<String, Object> records = consumer;
            stream.print(records);
        }
    }
    
}
