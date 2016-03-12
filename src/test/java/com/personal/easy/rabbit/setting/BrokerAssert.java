package com.personal.easy.rabbit.setting;

import com.personal.easy.rabbit.setting.BrokerConnection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;

import junit.framework.Assert;

import java.io.IOException;

/**
 * A broker assert provides a convenient way to check the
 * current state of the broker, especially the state
 * of its queues.
 *
 */
public class BrokerAssert {

    private Channel channel;

    public BrokerAssert() {
        this(BrokerConnection.getConnection());
    }

    public BrokerAssert(String host, int port) {
        this(BrokerConnection.getConnection(host, port));
    }

    public BrokerAssert(Connection connection) {
        try {
            channel = connection.createChannel();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create a new channel", e);
        }
    }
    
    /**
     * Asserts that the given queue DOES contain
     * at least one message.
     * 
     * @param queue The queue name
     * @throws IOException
     */
    public void queueNotEmtpy(String queue) throws IOException {
        GetResponse response = channel.basicGet(queue, false);
        Assert.assertNotNull(response);
        channel.basicNack(response.getEnvelope().getDeliveryTag(), false, true);
    }
    
    /**
     * Asserts that the given queue DOES NOT contain
     * any messages.
     * 
     * @param queue The queue name
     * @throws IOException
     */
    public void queueEmtpy(String queue) throws IOException {
        GetResponse response = channel.basicGet(queue, false);
        if (response != null) {
            channel.basicNack(response.getEnvelope().getDeliveryTag(), false, true);
        }
        Assert.assertNull(response);
    }
    
    /**
     * <p>Asserts that the given queue contains exactly
     * the given amount of messages.</p>
     * 
     * @param queue The queue name
     * @param expectedCount The expected amount of messages in the queue
     * @throws IOException
     */
    public void queueSize(String queue, int expectedCount) throws IOException {
        GetResponse response = channel.basicGet(queue, false);
        if (expectedCount == 0) {
            Assert.assertNull(response);
        } else {
            Assert.assertNotNull(response);
            try {
                Assert.assertEquals(expectedCount, response.getMessageCount()+1);
            } finally {
                channel.basicNack(response.getEnvelope().getDeliveryTag(), false, true);
            }
        }
    }
    
    /**
     * <p>Asserts that the first message in the queue equals
     * the given message.</p>
     * 
     * @param queue The queue name
     * @param message The message to assert being head of queue
     * @throws IOException
     */
    public void messageInQueue(String queue, String message) throws IOException {
        GetResponse response = channel.basicGet(queue, false);
        Assert.assertNotNull(response);
        try {
            Assert.assertEquals(message, new String(response.getBody(), "UTF-8"));
        } finally {
            channel.basicNack(response.getEnvelope().getDeliveryTag(), false, true);
        }
    }
    
    /**
     * <p>Asserts that the first messages in the queue equals
     * the given messages. 
     * 
     * @param queue The queue name
     * @param messages The messages to assert being head of queue
     * @throws IOException
     */
    public void messagesInQueue(String queue, String[] messages) throws IOException {
        long highestDeliveryTag = -1;
        try {
            for (String message : messages) {
                GetResponse response = channel.basicGet(queue, false);
                Assert.assertNotNull(response);
                long deliveryTag = response.getEnvelope().getDeliveryTag();
                highestDeliveryTag = Math.max(highestDeliveryTag, deliveryTag);
                Assert.assertEquals(message, new String(response.getBody(), "UTF-8"));
            }
        } finally {
            if (highestDeliveryTag >= 0) {
                channel.basicNack(highestDeliveryTag, true, true);
            }
        }
    }

}
