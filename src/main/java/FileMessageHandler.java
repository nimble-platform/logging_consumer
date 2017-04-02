import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by evgeniyh on 02/04/17.
 */
public class FileMessageHandler implements MessageHandler {
    private final static Logger logger = Logger.getLogger(FileMessageHandler.class);

    private final FileWriter fileWriter;
    private final BufferedWriter bufferedWriter;

    public FileMessageHandler(String filePath) throws IOException {
        fileWriter = new FileWriter(filePath, true);
        bufferedWriter = new BufferedWriter(fileWriter);
        logger.info("appending the logs to file " + filePath);
    }

    @Override
    public void handle(ConsumerRecords<String, String> records) {
        try {
            for (ConsumerRecord<String, String> record : records) {
                String jsonString = Utils.createJsonString(record.topic(), record.value(), record.timestamp());
                bufferedWriter.write(jsonString);
                bufferedWriter.newLine();
                logger.info("Sending message" + jsonString);
            }
            bufferedWriter.flush();
        } catch (Exception e) {
            logger.error("Error during sending message to file", e);
        }
    }

    @Override
    public void close() throws Exception {
        if (fileWriter != null) {
            fileWriter.close();
        }
        if (bufferedWriter != null) {
            bufferedWriter.close();
        }
    }
}
