import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.net.Socket;

/**
 * Created by evgeniyh on 02/04/17.
 */
public class LogstashMessageHandler implements MessageHandler {
    private final static Logger logger = Logger.getLogger(LogstashMessageHandler.class);

    private final String host;
    private final int port;

    public LogstashMessageHandler(String host, int port) {
        this.host = host;
        this.port = port;
        logger.info(String.format("Logstash host is '%s' and port '%s'", host, port));
    }

    @Override
    public void handle(ConsumerRecords<String, String> records) {
        try (Socket socket = new Socket(host, port);
             DataOutputStream os = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {
            for (ConsumerRecord<String, String> record : records) {
                String jsonString = Utils.createJsonString(record.topic(), record.value(), record.timestamp());
                os.writeBytes(jsonString + "\n");
                logger.info("Sending message" + jsonString);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error during handling records", e);
        }
    }

    @Override
    public void close() throws Exception {
        logger.info("Closing logstash message handler");
    }
}
