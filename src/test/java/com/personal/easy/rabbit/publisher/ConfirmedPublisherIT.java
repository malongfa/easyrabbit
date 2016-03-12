package com.personal.easy.rabbit.publisher;


import org.junit.Test;

import com.personal.easy.rabbit.Message;
import com.personal.easy.rabbit.TestBrokerSetup;
import com.personal.easy.rabbit.publisher.ConfirmedPublisher;


public class ConfirmedPublisherIT extends MessagePublisherIT {

    @Test
    public void shouldPublishMessage() throws Exception {
        ConfirmedPublisher publisher = new ConfirmedPublisher(singleConnectionFactory);
        Message message = new Message()
                .exchange(TestBrokerSetup.TEST_EXCHANGE)
                .routingKey(TestBrokerSetup.TEST_ROUTING_KEY)
                .body("abc");
        publisher.publish(message);
        Thread.sleep(100);
        brokerAssert.messageInQueue(TestBrokerSetup.TEST_QUEUE, message.getBodyAs(String.class));
    }

}
