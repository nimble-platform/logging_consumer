import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Created by evgeniyh on 23/03/17.
 */
public class LoggingConsumerTest {

    @Test
    public void testSubscribeToAllTopics() throws Exception {
//        Random randomGenerator = new Random();
//        String random = String.valueOf(randomGenerator.nextInt());
//        MessageCounter mc = new MessageCounter(TEST_MESSAGE_PREFIX + random);
//
//        LoggingConsumer allConsumer = new LoggingConsumer();
//        allConsumer.start();
//
//        String newTopic = TEST_TOPIC_PREFIX + random + "_0";
//        producer.sendMsgNoWait(newTopic, TEST_MESSAGE_PREFIX + random);
//
//        int timeToWaitForPropagation = 1002; // Needs to be bigger then metadata.max.age.ms property
//        while (timeToWaitForPropagation > 0) {
//            Thread.sleep(DEFAULT_SLEEP);
//            timeToWaitForPropagation -= DEFAULT_SLEEP;
//        }
//
//        Assert.assertTrue(allConsumer.getSubscribedTopics().contains(newTopic));
//        Assert.assertEquals(1, mc.getCounter());
    }
    
    
    
//    private class MessageCounter implements MessageHandler {
//        private final String expectedMsg;
//        private int counter = 0;
//
//        public MessageCounter(String expectedMsg) {
//            this.expectedMsg = expectedMsg;
//        }
//
//        @Override
//        synchronized public void handle(String message) {
//            if (message.equals(expectedMsg)) {
//                counter++;
//            }
//        }
//
//        public int getCounter() {
//            return counter;
//        }
//    }
}