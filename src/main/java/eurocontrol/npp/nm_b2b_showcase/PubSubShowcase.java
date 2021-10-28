package eurocontrol.npp.nm_b2b_showcase;

import javax.jms.*;

public class PubSubShowcase {

    public static String AMQPS_URL = "amqps://publish.nmvp.nm.eurocontrol.int:5671";
    public static String CERTIFICATE = "C:/Users/mjacques/IdeaProjects/nm_pubsub_showcase/src/main/resources/CC0000003728_519.p12";
    public static String PASSWORD = "************";
    public static String QUEUE_NAME = "LFPGDPIO.c5193728.FLIGHT_DATA.25.0.0.410e438c-354c-4e7f-8925-5637bca5be4e";

    public static void main(String... args) {
        AmqpClientConnectionManager connectionManager = null;
        try {
            connectionManager = new AmqpClientConnectionManager(AMQPS_URL, CERTIFICATE, PASSWORD);
            connectionManager.init();
            connectionManager.setMessageConsumerListener(QUEUE_NAME, new MessageListener() {

                @Override
                public void onMessage(Message message) {
                    try {
                        String messageBody = Utils.getMessageBody(message);
                        System.out.println(messageBody);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });

            Thread.currentThread().join();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (connectionManager != null) {
                try {
                    connectionManager.shutdown();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
}
