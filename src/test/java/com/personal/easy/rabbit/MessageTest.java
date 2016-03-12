package com.personal.easy.rabbit;

import com.personal.easy.rabbit.Message;
import com.personal.easy.rabbit.SingleConnectionFactory;
import com.personal.easy.rabbit.consumer.ConsumerContainer;
import com.personal.easy.rabbit.consumer.MessageCallback;
import com.personal.easy.rabbit.consumer.MessageConsumer;
import com.rabbitmq.client.ConnectionFactory;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;

public class MessageTest {

    Message message;
    
    @Before
    public void before() {
        message = new Message().exchange("exchange").routingKey("routingKey");
    }
    
    @Test
    public void shouldReturnString() {
        String bodyContent = "öüä";
        message.body(bodyContent);
        
        String actualBodyContent = message.getBodyAs(String.class);
        Assert.assertEquals(bodyContent, actualBodyContent);
    }
    
    @Test
    public void shouldReturnInteger() {
        int bodyContent = 123456;
        message.body("" + bodyContent);
        
        int actualBodyContent = message.getBodyAs(Integer.class);
        Assert.assertEquals(bodyContent, actualBodyContent);
    }
    
    @Test
    public void shouldReturnLong() {
        long bodyContent = 12345678901234l;
        message.body("" + bodyContent);

        long actualBodyContent = message.getBodyAs(Long.class);
        Assert.assertEquals(bodyContent, actualBodyContent);
    }

    @Test
    public void shouldSetPropertyDeliveryMode() {
        Message message = new Message().exchange("abc").routingKey("123").persistent();

        int deliveryMode = message.getBasicProperties().getDeliveryMode();
        Assert.assertEquals(Message.DELIVERY_MODE_PERSISTENT, deliveryMode);
    }

    @Test
    public void shouldSetPropertyCharset() {
        String charset = "ISO-8859-2";
        Message message = new Message().exchange("abc").routingKey("123")
                .body("abc", Charset.forName(charset));

        String actualCharset = message.getBasicProperties().getContentEncoding();
        Assert.assertEquals(charset, actualCharset);
    }

    public class MyConsumer implements MessageCallback {

        public void handleMessage(Message message) {
            String messageContent = message.getBodyAs(String.class);
            System.out.println(messageContent);
        }
    }

 
    public void test() throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new SingleConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);
        
        ConsumerContainer consumerContainer = new ConsumerContainer(connectionFactory);
        consumerContainer.addConsumer(new MyConsumer(), "my.queue", true);
        consumerContainer.startAllConsumers();
    }

}
