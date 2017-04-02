import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Created by evgeniyh on 02/04/17.
 */
public interface MessageHandler extends AutoCloseable {
    void handle(ConsumerRecords<String, String> records);
}
