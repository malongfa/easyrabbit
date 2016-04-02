package com.personal.easy.rabbit.publisher;


import java.io.IOException;

import org.junit.Test;

import com.personal.easy.rabbit.TestBrokerSetup;
import com.personal.easy.rabbit.connection.SingleConnectionFactory;
import com.personal.easy.rabbit.message.Message;
import com.rabbitmq.client.Connection;


public class TransactionalPublisherIT extends MessagePublisherIT {

    @Test
    public void shouldPublishMessage() throws Exception {
        TransactionalPublisher publisher = new TransactionalPublisher(this.singleConnectionFactory);
        Message message = new Message()
                .exchange(TestBrokerSetup.TEST_EXCHANGE)
                .routingKey(TestBrokerSetup.TEST_ROUTING_KEY)
                .body("abc");
        publisher.publish(message);
        Thread.sleep(100);
        this.brokerAssert.messageInQueue(TestBrokerSetup.TEST_QUEUE, message.getBodyAs(String.class));
    }

    @Test
    public void shouldRecoverFromConnectionLoss() throws Exception {
        TransactionalPublisher publisher = new TransactionalPublisher(this.singleConnectionFactory);
        Message message = new Message()
                .exchange(TestBrokerSetup.TEST_EXCHANGE)
                .routingKey(TestBrokerSetup.TEST_ROUTING_KEY);

        publisher.publish(message);
        Thread.sleep(100);
        this.brokerAssert.queueSize(TestBrokerSetup.TEST_QUEUE, 1);
        Connection connection = this.singleConnectionFactory.newConnection();
        this.singleConnectionFactory.setPort(15345);
        connection.close();
        int waitForReconnects = SingleConnectionFactory.CONNECTION_ESTABLISH_INTERVAL_IN_MS + SingleConnectionFactory.CONNECTION_TIMEOUT_IN_MS * 2;
        Thread.sleep(waitForReconnects);
        try {
            publisher.publish(message);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.singleConnectionFactory.setPort(this.brokerSetup.getPort());
        Thread.sleep(waitForReconnects);
        publisher.publish(message);
        Thread.sleep(waitForReconnects);
        this.brokerAssert.queueSize(TestBrokerSetup.TEST_QUEUE, 2);
    }

}
