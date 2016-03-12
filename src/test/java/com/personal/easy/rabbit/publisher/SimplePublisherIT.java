package com.personal.easy.rabbit.publisher;

import com.personal.easy.rabbit.Message;
import com.personal.easy.rabbit.TestBrokerSetup;
import com.personal.easy.rabbit.publisher.SimplePublisher;

import org.junit.Test;

public class SimplePublisherIT extends MessagePublisherIT {

    @Test
    public void shouldPublishMessage() throws Exception {
        SimplePublisher publisher = new SimplePublisher(singleConnectionFactory);
        Message message = new Message()
                .exchange(TestBrokerSetup.TEST_EXCHANGE)
                .routingKey(TestBrokerSetup.TEST_ROUTING_KEY)
                .body("abc");

        publisher.publish(message);
        Thread.sleep(100);
        brokerAssert.messageInQueue(TestBrokerSetup.TEST_QUEUE, message.getBodyAs(String.class));
    }

}
