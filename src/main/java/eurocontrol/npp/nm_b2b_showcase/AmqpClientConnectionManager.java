package eurocontrol.npp.nm_b2b_showcase;

import javax.jms.*;
import javax.naming.Context;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.logging.Logger;

public class AmqpClientConnectionManager {

    private Logger logger = Logger.getLogger(getClass().getName());

    private String amqpsUrl;
    private String certificateFilename;
    private String certificatePassword;

    private Connection connection;
    private Session session;
    private List<MessageConsumer> nmAmqpConsumers;

    public AmqpClientConnectionManager(String amqpsUrl, String certificateFilename, String certificatePassword) {
        this.amqpsUrl = amqpsUrl;
        this.certificateFilename = certificateFilename;
        this.certificatePassword = certificatePassword;
    }
    
    public void init() {
        logger.info("Instantiating Amqp Client Connection Manager...");
        nmAmqpConsumers = new ArrayList<>();
    }

    public void shutdown() throws Exception {
    	logger.info("Shutting down Amqp Client Connection Manager...");
        synchronized (nmAmqpConsumers) {
            for (MessageConsumer consumer : nmAmqpConsumers) {
                consumer.close();
            }
            nmAmqpConsumers.clear();
        }
        if (session != null) {
            session.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    public boolean setMessageConsumerListener(String queueName, MessageListener messageListener) {
        try {
            Session session = getSession();
            if (session == null)
                return false;
            synchronized (session) {
                Queue queue = session.createQueue(queueName);
                MessageConsumer consumer = session.createConsumer(queue);
                consumer.setMessageListener(messageListener);
                nmAmqpConsumers.add(consumer);
                return true;
            }
        } catch (JMSException e) {
            logger.severe("Error creating consumer: " + e.getMessage());
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Setup a connection for the given certificate
     */
    private Connection getConnection() {

        try {
            List<String> queryVariables = new ArrayList<>();
            queryVariables.add("transport.tcpKeepAlive=true");
            queryVariables.add("transport.keyStoreLocation=" + this.certificateFilename);
            queryVariables.add("transport.keyStorePassword=" + this.certificatePassword);
            queryVariables.add("transport.trustAll=true");

            String connectionString = this.amqpsUrl;
            if (!queryVariables.isEmpty()) {
                connectionString += "?" + Utils.concat("&", queryVariables);
            }
            connectionString = "failover:(" + connectionString + ")"; // Configure failover url to reconnect automatically if connection is lost

            logger.info("AMQP Connection URL : " + connectionString);
            Hashtable<Object, Object> env = new Hashtable<>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
            env.put("connectionfactory.qpidConnectionFactory", connectionString);

            Context context = new javax.naming.InitialContext(env);
            ConnectionFactory connectionFactory
                    = (ConnectionFactory) context.lookup("qpidConnectionFactory");
            Connection connection = connectionFactory.createConnection();
            connection.setExceptionListener(new ExceptionListener() {
                @Override
                public void onException(JMSException e) {
                    logger.severe("Connection to NM AMQPS had an exception: " + e.getMessage());
                    e.printStackTrace();
                }
            });
            connection.setClientID("NPP_TEST");
            connection.start();
            return connection;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    private Session getSession() {
        if (session == null) {

            connection = getConnection();
            if (connection == null) {
                return null;
            }
            try {
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            } catch (JMSException e) {
                e.printStackTrace();
            }

            return session;
        }
        return session;
    }

}
