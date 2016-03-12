package com.personal.easy.rabbit.setting;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.*;

/**
 * <p>A broker setup is used for integration tests for
 * easily setting up exchanges and queues in a broker 
 * during a test and removing them afterwards properly.</p>
 * 
 * <p>In a JUnit test, it is recommended to use the declaring methods
 * (e.g. {@link #declareAndBindQueue(String, String, String)} ) 
 * of a broker setup in the @Before or @BeforeClass phase.
 * In the @After or @AfterClass phase, use the {@link #tearDown()}
 * method to clean up the broker after your tests have run.</p>
 *
 */
public class BrokerSetup {
    
    public static final String DEAD_LETTER_EXCHANGE = "x-dead-letter-exchange";
    public static final String DEAD_LETTER_ROUTING_KEY = "x-dead-letter-routing-key";
    public static final String HIGH_AVAILABILITY_POLICY = "x-ha-policy";

    private Channel channel;
    
    private List<String> declaredExchanges = new LinkedList<String>();
    private List<String> declaredQueues = new LinkedList<String>();
    
    public BrokerSetup() {
        this(BrokerConnection.getConnection());
    }

    public BrokerSetup(String host, int port) {
        this(BrokerConnection.getConnection(host, port));
    }

    public BrokerSetup(Connection connection) {
        try {
            channel = connection.createChannel();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create a new channel", e);
        }
    }
    
    /**
     * Declares an exchange of the given exchange type (e.g. topic, fanout, ...)
     * on the broker.
     * 
     * @param exchange The exchange name
     * @param type The exchange type
     * @throws IOException if the declaration of the exchange fails
     */
    public void declareExchange(String exchange, String type) throws IOException {
        //channel.exchangeDeclare(exchange, type);
    	channel.exchangeDeclare(exchange, type,true);
        declaredExchanges.add(exchange);
    }
    
    
    /**
     * Declares a queue using no arguments.
     * 
     * @param queue The queue name
     * @throws IOException if the declaration of the queue fails
     */
    public void declareQueue(String queue) throws IOException {
        Map<String, Object> emptyArguments = Collections.<String,Object>emptyMap();
        declareQueue(queue, emptyArguments);
    }
    
    /**
     * Declares a queue using the given arguments.
     * 
     * @param queue The queue name
     * @param arguments The queue arguments
     * @throws IOException if the declaration of the queue fails
     */
    public void declareQueue(String queue, Map<String, Object> arguments) throws IOException {
        channel.queueDeclare(queue, false, false, false, arguments);
        declaredQueues.add(queue);
    }
    
    /**
     * Declares a queue and its dead letter queue and configures the real queue
     * for dead lettering.
     * 
     * @param queue The queue name
     * @throws IOException if the declaration of the queue or dead letter queue fails
     */
    public void declareQueueWithDeadLettering(String queue) throws IOException {
        String deadLetterQueue = queue + ":dead";
        Map<String, Object> deadLetterArguments = new HashMap<String, Object>();
        deadLetterArguments.put(DEAD_LETTER_EXCHANGE, "");
        deadLetterArguments.put(DEAD_LETTER_ROUTING_KEY, deadLetterQueue);
        declareQueue(queue, deadLetterArguments);
        declareQueue(deadLetterQueue);
    }
    
    /**
     * Declares a high availability queue.
     * 
     * @param queue The queue name
     * @throws IOException if the declaration of the queue fails
     */
    public void declareQueueWithHighAvailability(String queue) throws IOException {
        Map<String, Object> highAvailabilityArguments = new HashMap<String, Object>();
        highAvailabilityArguments.put(HIGH_AVAILABILITY_POLICY, "all");
        declareQueue(queue, highAvailabilityArguments);
    }

    /**
     * Declares a queue and binds it to the given exchange with
     * the given routing key.
     * 
     * @param queue The queue name
     * @param exchange The exchange name
     * @param routingKey The routing key to bind
     * @throws IOException if the declaration of the queue or binding fails
     */
    public void declareAndBindQueue(String queue, String exchange, String routingKey) throws IOException {
        declareQueue(queue);
        channel.queueBind(queue, exchange, routingKey);
    }
    
    /**
     * Declares a queue and its dead letter queue and configures the real queue
     * for dead lettering. Binds the queue to the given exchange with
     * the given routing key.
     * 
     * @param queue The queue name
     * @param exchange The exchange name
     * @param routingKey The routing key to bind 
     * @throws IOException if the declaration of the queue, the dead letter queue or binding fails
     */
    public void declareAndBindQueueWithDeadLettering(String queue, String exchange, String routingKey) throws IOException {
        declareQueueWithDeadLettering(queue);
        channel.queueBind(queue, exchange, routingKey);
    }
    
    /**
     * Declares a high availability queue. Binds the queue to the given exchange with
     * the given routing key.
     * 
     * @param queue The queue name
     * @param exchange The exchange name
     * @param routingKey The routing key to bind
     * @throws IOException if the declaration of the queue or binding fails
     */
    public void declareAndBindQueueWithHighAvailability(String queue, String exchange, String routingKey) throws IOException {
        declareQueueWithHighAvailability(queue);
        channel.queueBind(queue, exchange, routingKey);
    }
    
    /**
     * Removes all declared exchanges, queue and bindings
     * declared with the instance of this broker setup.
     * 
     * @throws IOException if the removing of an exchange or queue fails
     */
    public void tearDown() throws IOException {
        for (String exchange : declaredExchanges) {
            channel.exchangeDelete(exchange);
        }
        for (String queue : declaredQueues) {
            channel.queueDelete(queue);
        }
    }
    
    /**
     * Gets the channel used by the broker setup.
     * 
     * @return The channel
     */
    public Channel getChannel() {
        return channel;
    }

    /**
     * Gets the host of the used broker.
     * 
     * @return the host
     */
    public String getHost() {
        return channel.getConnection().getAddress().getHostName();
    }

    /**
     * Gets the port of the used broker.
     * 
     * @return the port
     */
    public int getPort() {
        return channel.getConnection().getPort();
    }
    
    
}
