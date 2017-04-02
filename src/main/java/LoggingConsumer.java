import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

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
        Properties prop = Utils.loadProperties(Utils.CONSUMER_DEV);
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumer = new KafkaConsumer<>(prop);
    }

    public void start(MessageHandler messageHandler) {
        validateCanBeStarted();
        activated = true;
        logger.info("Registering to all of the topics");
        consumer.subscribe(Pattern.compile(".*"), new ConsumerRebalanceListenerImpl());

        ConsumerRecords<String, String> records;
        while (!closed) {
            records = consumer.poll(100);
            if (!records.isEmpty()) {
                messageHandler.handle(records);
            }
        }
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
