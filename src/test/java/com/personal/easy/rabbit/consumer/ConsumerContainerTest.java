package com.personal.easy.rabbit.consumer;

import com.personal.easy.rabbit.Message;
import com.personal.easy.rabbit.TestBrokerSetup;
import com.personal.easy.rabbit.consumer.ConsumerContainer;
import com.personal.easy.rabbit.consumer.MessageCallback;
import com.personal.easy.rabbit.consumer.MessageConsumer;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import junit.framework.Assert;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.List;

import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;


@RunWith(PowerMockRunner.class)
public class ConsumerContainerTest {
    
    private ConsumerContainer consumerContainer;
    
    @Mock
    private ConnectionFactory connectionFactory;
    @Mock
    private Connection connection;
    @Mock
    private Channel channel;

    @Before
    public void before() throws IOException {
        consumerContainer = new ConsumerContainer(connectionFactory);
        consumerContainer.addConsumer(new TestConsumerOne(), TestBrokerSetup.TEST_QUEUE);
        consumerContainer.addConsumer(new TestConsumerTwo(), TestBrokerSetup.TEST_QUEUE);
    }
    
    private void mockConnectionOperations() throws Exception {
        expect(connectionFactory.newConnection()).andReturn(connection).anyTimes();
        expect(connection.createChannel()).andReturn(channel).anyTimes();
        channel.close();
        expectLastCall().anyTimes();
    }
    
    private void mockCheckingOperations() throws Exception {
        expect(channel.queueDeclarePassive(anyObject(String.class))).andReturn(null).anyTimes();
    }
    
    private void mockActivatingOperations() throws Exception {
        expect(channel.basicConsume(anyObject(String.class), anyBoolean(), isA(MessageConsumer.class))).andReturn("").anyTimes();
        channel.basicQos(EasyMock.anyInt());
        EasyMock.expectLastCall().atLeastOnce();
    }
    
    private void mockDeactivatingOperations() throws Exception {
        expect(channel.getCloseReason()).andReturn(null).anyTimes();
    }

//    @Test
//    public void testAddConsumerForSingleConsumer() throws Exception {
//        consumerContainer.addConsumer(new TestConsumerThree(), TestBrokerSetup.TEST_QUEUE);
//        Assert.assertEquals(consumerContainer.filterConsumersForClass(TestConsumerThree.class).size(), 1);
//    }

//    @Test
//    public void testAddConsumerForMultipleConsumers() throws Exception {
//        int instanceAmount = 10;
//        consumerContainer.addConsumer(new TestConsumerThree(), TestBrokerSetup.TEST_QUEUE, instanceAmount);
//        Assert.assertEquals(consumerContainer.filterConsumersForClass(TestConsumerThree.class).size(), instanceAmount);
//    }
    
//    @Test
//    public void testFilterConsumersForClass() {
//        int consumerCount = consumerContainer.filterConsumersForClass(MessageConsumer.class).size();
//        Assert.assertEquals(2, consumerCount);
//        consumerCount = consumerContainer.filterConsumersForClass(TestConsumerOne.class).size();
//        Assert.assertEquals(1, consumerCount);
//        consumerCount = consumerContainer.filterConsumersForClass(TestConsumerTwo.class).size();
//        Assert.assertEquals(1, consumerCount);
//    }
    
    @Test
    public void testFilterConsumersForEnabledFlag() {
        List<ConsumerContainer.ConsumerHolder> consumerHolderList = consumerContainer.consumerHolders;
        consumerHolderList.get(0).enabled = true;
        consumerHolderList.get(1).enabled = false;
        int enabledInnerConsumerCount = consumerContainer.filterConsumersForEnabledFlag(true).size();
        Assert.assertEquals(1, enabledInnerConsumerCount);
        int disabledInnerConsumerCount = consumerContainer.filterConsumersForEnabledFlag(false).size();
        Assert.assertEquals(1, disabledInnerConsumerCount);
    }
    
    @Test
    public void testFilterConsumersForActiveFlag() {
        List<ConsumerContainer.ConsumerHolder> consumerHolderList = consumerContainer.consumerHolders;
        consumerHolderList.get(0).active = true;
        consumerHolderList.get(1).active = false;
        int activeConsumerHolderSize = consumerContainer.filterConsumersForActiveFlag(true).size();
        Assert.assertEquals(1, activeConsumerHolderSize);
        int inactiveConsumerHolderSize = consumerContainer.filterConsumersForActiveFlag(false).size();
        Assert.assertEquals(1, inactiveConsumerHolderSize);
    }
    
    @Test
    public void testActivateConsumer() throws Exception {
        mockConnectionOperations();
        @SuppressWarnings("unchecked")
        ConsumerContainer.ConsumerHolder consumerHolder = consumerContainer.consumerHolders.get(0);
        expect(channel.basicConsume(TestBrokerSetup.TEST_QUEUE, false, consumerHolder.getConsumer())).andReturn("").once();
        channel.basicQos(EasyMock.anyInt());
        EasyMock.expectLastCall().once();
        PowerMock.replayAll();
        consumerHolder.activate();
        Assert.assertTrue(consumerHolder.isActive());
        PowerMock.verifyAll();
    }
    
    @Test
    public void testDeactivateConsumer() throws Exception {
        mockConnectionOperations();
        mockActivatingOperations();
        mockDeactivatingOperations();
        PowerMock.replayAll();
        @SuppressWarnings("unchecked")
        ConsumerContainer.ConsumerHolder consumerHolder = consumerContainer.consumerHolders.get(0);
        consumerHolder.activate();
        Assert.assertTrue(consumerHolder.isActive());
        consumerHolder.deactivate();
        Assert.assertFalse(consumerHolder.isActive());
        PowerMock.verifyAll();
    }
    
    @Test
    public void testGetDisabledConsumers() {
        List<ConsumerContainer.ConsumerHolder> consumerHolders = consumerContainer.getDisabledConsumers();
        Assert.assertEquals(2, consumerHolders.size());
    }
    
    @Test
    public void testGetInactiveConsumers() {
        List<ConsumerContainer.ConsumerHolder> consumerHolders = consumerContainer.getInactiveConsumers();
        Assert.assertEquals(2, consumerHolders.size());
    }
    
    @Test
    public void testGetEnabledConsumers() {
        List<ConsumerContainer.ConsumerHolder> consumerHolderList = consumerContainer.consumerHolders;
        consumerHolderList.get(0).enabled = true;
        consumerHolderList.get(1).enabled = true;
        List<ConsumerContainer.ConsumerHolder> consumerHolders = consumerContainer.getEnabledConsumers();
        Assert.assertEquals(2, consumerHolders.size());
    }
    
    @Test
    public void testGetActiveConsumers() {
        List<ConsumerContainer.ConsumerHolder> consumerHolderList = consumerContainer.consumerHolders;
        consumerHolderList.get(0).active = true;
        consumerHolderList.get(1).active = true;
        List<ConsumerContainer.ConsumerHolder> consumers = consumerContainer.getActiveConsumers();
        Assert.assertEquals(2, consumers.size());
    }
    
    @Test
    public void testStartAllConsumers() throws Exception {
        mockConnectionOperations();
        mockCheckingOperations();
        mockActivatingOperations();
        PowerMock.replayAll();
        consumerContainer.startAllConsumers();
        int enabledConsumerCount = consumerContainer.getEnabledConsumers().size();
        Assert.assertEquals(2, enabledConsumerCount);
        int activeConsumerCount = consumerContainer.getActiveConsumers().size();
        Assert.assertEquals(2, activeConsumerCount);
        PowerMock.verifyAll();
    }
    
    @Test
    public void testStopAllConsumers() throws Exception {
        mockConnectionOperations();
        mockCheckingOperations();
        mockActivatingOperations();
        mockDeactivatingOperations();
        PowerMock.replayAll();
        consumerContainer.startAllConsumers();
        consumerContainer.stopAllConsumers();
        int disabledConsumerCount = consumerContainer.getDisabledConsumers().size();
        Assert.assertEquals(2, disabledConsumerCount);
        int inactiveConsumerCount = consumerContainer.getInactiveConsumers().size();
        Assert.assertEquals(2, inactiveConsumerCount);
        PowerMock.verifyAll();
    }
    
//    @Test
//    public void testStartConsumers() throws Exception {
//        mockConnectionOperations();
//        mockCheckingOperations();
//        mockActivatingOperations();
//        PowerMock.replayAll();
//        consumerContainer.startConsumers(TestConsumerOne.class);
//        int enabledConsumerCount = consumerContainer.getEnabledConsumers().size();
//        Assert.assertEquals(1, enabledConsumerCount);
//        consumerContainer.startConsumers(TestConsumerTwo.class);
//        enabledConsumerCount = consumerContainer.getEnabledConsumers().size();
//        Assert.assertEquals(2, enabledConsumerCount);
//    }
    
//    @Test
//    public void testStopConsumers() throws Exception {
//        mockConnectionOperations();
//        mockCheckingOperations();
//        mockActivatingOperations();
//        mockDeactivatingOperations();
//        PowerMock.replayAll();
//        consumerContainer.startAllConsumers();
//        consumerContainer.stopConsumers(TestConsumerOne.class);
//        int enabledConsumerCount = consumerContainer.getEnabledConsumers().size();
//        Assert.assertEquals(1, enabledConsumerCount);
//        consumerContainer.stopConsumers(TestConsumerTwo.class);
//        enabledConsumerCount = consumerContainer.getEnabledConsumers().size();
//        Assert.assertEquals(0, enabledConsumerCount);
//    }
    
    public class TestConsumerOne implements MessageCallback {

        public void handleMessage(Message message) { }
    }

//    public class TestConsumerOne extends MessageConsumer {
//
//        public void handleMessage(Message message) { }
//    }
    
    public class TestConsumerTwo implements MessageCallback  {

        public void handleMessage(Message message) { }
        
    }

    public class TestConsumerThree implements MessageCallback  {

        public void handleMessage(Message message) { }

    }
    

}
