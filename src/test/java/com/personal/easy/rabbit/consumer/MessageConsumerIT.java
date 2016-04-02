package com.personal.easy.rabbit.consumer;


import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.personal.easy.rabbit.TestBrokerSetup;
import com.personal.easy.rabbit.connection.SingleConnectionFactory;
import com.personal.easy.rabbit.setting.BrokerAssert;
import com.personal.easy.rabbit.setting.BrokerSetup;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;

import junit.framework.Assert;


public class MessageConsumerIT {

    BrokerSetup brokerSetup;
    BrokerAssert brokerAssert;
    SingleConnectionFactory connectionFactory;
    
    @Before
    public void before() throws Exception {
        this.brokerAssert = new BrokerAssert();
        this.brokerSetup = new BrokerSetup();
        this.brokerSetup.declareQueueWithDeadLettering(TestBrokerSetup.TEST_QUEUE);
        this.connectionFactory = new SingleConnectionFactory();
        this.connectionFactory.setHost(this.brokerSetup.getHost());
        this.connectionFactory.setPort(this.brokerSetup.getPort());
    }
    
    @After
    public void after() throws Exception {
        this.brokerSetup.tearDown();
    }
    
    @Test
    public void shouldConsumeMessage() throws Exception {
        sendTestMessage();
        Channel consumerChannel = this.connectionFactory.newConnection().createChannel();
        AckingConsumer consumer = new AckingConsumer();
        consumer.setChannel(consumerChannel);
        consumer.setConfiguration(new ConsumerConfiguration(TestBrokerSetup.TEST_QUEUE, true));
        consumerChannel.basicConsume(TestBrokerSetup.TEST_QUEUE, consumer);
        Thread.sleep(100);
        Assert.assertTrue(consumer.called);
        this.brokerAssert.queueEmtpy(TestBrokerSetup.TEST_QUEUE);
        this.brokerAssert.queueEmtpy(TestBrokerSetup.TEST_QUEUE_DEAD);
    }
    
    @Test
    public void shouldPutMessageToDeadLetterQueue() throws Exception {
        sendTestMessage();
        Channel consumerChannel = this.connectionFactory.newConnection().createChannel();
        NackingConsumer consumer = new NackingConsumer();
        consumer.setChannel(consumerChannel);
        consumer.setConfiguration(new ConsumerConfiguration(TestBrokerSetup.TEST_QUEUE, false));
        consumerChannel.basicConsume(TestBrokerSetup.TEST_QUEUE, consumer);
        Thread.sleep(100);
        Assert.assertTrue(consumer.called);
        this.brokerAssert.queueEmtpy(TestBrokerSetup.TEST_QUEUE);
        this.brokerAssert.queueNotEmtpy(TestBrokerSetup.TEST_QUEUE_DEAD);
    }
    
    private void sendTestMessage() throws IOException, InterruptedException, TimeoutException {
        Channel producerChannel = this.connectionFactory.newConnection().createChannel();
        producerChannel.confirmSelect();
        producerChannel.basicPublish("", TestBrokerSetup.TEST_QUEUE, new BasicProperties.Builder().build(), "test".getBytes("UTF-8"));
        producerChannel.waitForConfirmsOrDie();
        this.brokerAssert.queueNotEmtpy(TestBrokerSetup.TEST_QUEUE);
    }
    
    private class NackingConsumer extends MessageConsumer {
        boolean called = false;
    }
    
    private class AckingConsumer extends MessageConsumer {
        boolean called = false;
        
    }
    
}
