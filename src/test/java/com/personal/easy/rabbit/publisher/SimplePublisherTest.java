package com.personal.easy.rabbit.publisher;


import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import com.personal.easy.rabbit.Message;
import com.personal.easy.rabbit.publisher.SimplePublisher;

import static org.easymock.EasyMock.expectLastCall;

public class SimplePublisherTest extends MessagePublisherTest {
    
    @Test
    public void shouldPublishMessage() throws Exception {
        Message message = new Message().exchange(TEST_EXCHANGE).routingKey(TEST_ROUTING_KEY);
        SimplePublisher publisher = new SimplePublisher(connectionFactory);
        
        mockConnectionOperations();
        channel.basicPublish(TEST_EXCHANGE, TEST_ROUTING_KEY, false, false, message.getBasicProperties(), message.getBodyContent());
        expectLastCall().once();
        PowerMock.replayAll();
        
        publisher.publish(message);
        
        PowerMock.verifyAll();
    }

}
