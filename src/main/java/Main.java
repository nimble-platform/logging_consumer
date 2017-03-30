import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

/**
 * Created by evgeniyh on 22/03/17.
 */
public class Main {
    public static void main(String[] args) {
        try (LoggingConsumer consumer = new LoggingConsumer()) {
            consumer.start("192.168.1.2", 5000);
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("CLI Producer").description("Will send data to topic");
        parser.addArgument("topic_name").help("The name of the target topic");
        parser.addArgument("message").help("The data to be send");

        try {
            return parser.parseArgs(args);
        } catch (ArgumentParserException ex) {
            parser.handleError(ex);
            return null;
        }
    }
}
