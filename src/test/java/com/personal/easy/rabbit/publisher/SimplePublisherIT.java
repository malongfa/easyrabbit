package com.personal.easy.rabbit.publisher;

import org.junit.Test;

import com.personal.easy.rabbit.TestBrokerSetup;
import com.personal.easy.rabbit.message.Message;

public class SimplePublisherIT extends MessagePublisherIT {

    @Test
    public void shouldPublishMessage() throws Exception {
        SimplePublisher publisher = new SimplePublisher(this.singleConnectionFactory);
        Message message = new Message()
                .exchange(TestBrokerSetup.TEST_EXCHANGE)
                .routingKey(TestBrokerSetup.TEST_ROUTING_KEY)
                .body("abc");

        publisher.publish(message);
        Thread.sleep(100);
        this.brokerAssert.messageInQueue(TestBrokerSetup.TEST_QUEUE, message.getBodyAs(String.class));
    }

}
