import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.Collection;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Created by evgeniyh on 22/03/17.
 */
public class LoggingConsumer implements AutoCloseable {
    private final static Logger logger = Logger.getLogger(LoggingConsumer.class);
    private final KafkaConsumer<String, String> consumer;
    private final String groupId = "logging_consumer_e7badfa3-74e8-4c57-87a1-cd2abd6ec26b";
    private boolean activated;
    private boolean closed;

    public LoggingConsumer() {
        Properties prop = PropertiesLoader.loadProperties(PropertiesLoader.CONSUMER_DEV);
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumer = new KafkaConsumer<>(prop);
    }

    public void start(String logstashIp, int logstashPort) {
        validateCanBeStarted();
        activated = true;
        logger.info("Registering to all of the topics");
        consumer.subscribe(Pattern.compile(".*"), new ConsumerRebalanceListenerImpl());

        ConsumerRecords<String, String> records;
        while (!closed) {
            records = consumer.poll(100);
            if (records.isEmpty()) {
                continue;
            }
            try (Socket socket = new Socket(logstashIp, logstashPort);
                 DataOutputStream os = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {
                for (ConsumerRecord<String, String> record : records) {
                    String jsonString = createJson(record.topic(), record.value(), record.timestamp());
                    os.writeBytes(jsonString + "\n");
                    System.out.println("Sending message" + jsonString);
                }
            } catch (Exception e) {

            }
        }
    }

    private String createJson(String topic, String value, long timestamp) {
        return new JSONObject()
                .put("Topic", topic)
                .put("Message", value)
                .put("Timestamp", String.valueOf(timestamp)).toString();
    }

    private void validateCanBeStarted() {
        if (activated) {
            throw new IllegalAccessError("Start can be called only once");
        }
        if (closed) {
            throw new IllegalAccessError("Can't call start after the consumer was closed");
        }
    }

    @Override
    public void close() {
        if (!activated) {
            throw new IllegalAccessError("Can't close without calling start");
        }
        closed = true;
        consumer.close();
    }

    private class ConsumerRebalanceListenerImpl implements ConsumerRebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        }
    }
}
