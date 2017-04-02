import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.varia.NullAppender;

import java.io.IOException;

/**
 * Created by evgeniyh on 22/03/17.
 */
public class Main {
    private static Logger logger;

    private static LoggingConsumer consumer;

    //    TODO: maybe add object store storage
    public static void main(String[] args) {
        Logger.getRootLogger().addAppender(new NullAppender());
        logger = Logger.getLogger(Main.class);

        BasicConfigurator.configure();
        ArgumentParser parser = getParser();
        try {
            Namespace namespace = parser.parseArgs(args);
            MessageHandler messageHandler = getMessageHandler(namespace);
            consumer = new LoggingConsumer();
            consumer.start(messageHandler);
        } catch (ArgumentParserException ex) {
            parser.handleError(ex);
        } catch (Exception ex) {
            logger.error("Error in logging consumer", ex);
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    private static MessageHandler getMessageHandler(Namespace namespace) throws IOException {
        String loggingType = namespace.getString("command");
        switch (loggingType) {
            case ("file"):
                String filePath = namespace.getString("file_path");
                return new FileMessageHandler(filePath);
            case ("logstash"):
                String host = namespace.getString("host");
                int port = namespace.getInt("port");
                if (Utils.isServerUp(host, port)) {
                    return new LogstashMessageHandler(host, port);
                } else {
                    throw new RuntimeException("Logstash server seems to be unreachable");
                }
            default:
                throw new IllegalArgumentException(loggingType + "Isn't supported");
        }
    }

    private static ArgumentParser getParser() {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("Logging Consumer");

        Subparsers subparsers = parser.addSubparsers().title("Consumer Commands").description("Available logging commands");
        Subparser listTopics = subparsers.addParser("file");
        listTopics.setDefault("command", "file");
        listTopics.addArgument("file_path").help("Path to the local file");

        Subparser subscribeParser = subparsers.addParser("logstash");
        subscribeParser.setDefault("command", "logstash");
        subscribeParser.addArgument("host").help("Logstash hostname or ip");
        subscribeParser.addArgument("port").help("Logstash listening port").type(Integer.class);

        return parser;
    }
}
