import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;

/**
 * Created by evgeniyh on 22/03/17.
 */
public class Main {
    //    TODO: maybe add object store storage
    public static void main(String[] args) {
        ArgumentParser parser = getParser();
        try (LoggingConsumer consumer = new LoggingConsumer()) {
            Namespace namespace = parser.parseArgs(args);
            MessageHandler messageHandler = getMessageHandler(namespace);
            consumer.start(messageHandler);
        } catch (ArgumentParserException ex) {
            parser.handleError(ex);
        } catch (Exception ex) {
            System.out.println("Exiting");
            ex.printStackTrace();
        }
    }

    private static MessageHandler getMessageHandler(Namespace namespace) {
        String loggingType = namespace.getString("command");
        switch (loggingType) {
            case ("file"):
                String filePath = namespace.getString("file_path");
                return new FileMessageHandler(filePath);
            case ("logstash"):
                String host = namespace.getString("host");
                int port = namespace.getInt("port");
                return new LogstashMessageHandler(host, port);
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
