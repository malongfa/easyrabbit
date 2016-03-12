package com.personal.easy.rabbit.publisher;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;

import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import com.personal.easy.rabbit.Message;
import com.personal.easy.rabbit.publisher.ConfirmedPublisher;



public class ConfirmedPublisherTest extends MessagePublisherTest {
    
    @Test
    public void shouldPublishMessageAndWaitForAck() throws Exception {
        Message message = new Message().exchange(TEST_EXCHANGE).routingKey(TEST_ROUTING_KEY);
        ConfirmedPublisher publisher = new ConfirmedPublisher(connectionFactory);
        
        mockConnectionOperations();
        expect(channel.confirmSelect()).andReturn(null);
        channel.basicPublish(TEST_EXCHANGE, TEST_ROUTING_KEY, false, false, message.getBasicProperties(), message.getBodyContent());
        expectLastCall().once();
        channel.waitForConfirmsOrDie();
        expectLastCall().once();
        PowerMock.replayAll();
        
        publisher.publish(message);
        
        PowerMock.verifyAll();
    }

}
