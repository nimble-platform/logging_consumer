import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by evgeniyh on 09/03/17.
 */
public class Utils {
    final private static Logger logger = Logger.getLogger("basic");

    public static String CONSUMER_DEV = "consumer_dev.properties";

    public static Properties loadProperties(String file) {
        InputStream inputStream = Utils.class.getClassLoader().getResourceAsStream(file);
        try {
            if (inputStream == null) {
                throw new FileNotFoundException("property file '" + file + "' not found in the classpath");
            }
            Properties prop = new Properties();
            prop.load(inputStream);

            return prop;
        } catch (Exception ex) {
            logger.error(String.format("Exception '%s' on loading properties file '%s'", ex.getMessage(), file));
            throw new RuntimeException("Unable to load properties");
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    public static String createJsonString(String topic, String value, long timestamp) {
        return new JSONObject()
                .put("Topic", topic)
                .put("Message", value)
                .put("Timestamp", String.valueOf(timestamp)).toString();
    }
}
