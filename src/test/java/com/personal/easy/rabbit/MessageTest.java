package com.personal.easy.rabbit;


import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Test;

import com.personal.easy.rabbit.connection.SingleConnectionFactory;
import com.personal.easy.rabbit.consumer.ConsumerContainer;
import com.personal.easy.rabbit.consumer.MessageCallback;
import com.personal.easy.rabbit.message.Message;
import com.rabbitmq.client.ConnectionFactory;

import junit.framework.Assert;

public class MessageTest {

    Message message;

    @Before
    public void before() {
        this.message = new Message().exchange("exchange").routingKey("routingKey");
    }

    @Test
    public void shouldReturnString() {
        String bodyContent = "öüä";
        this.message.body(bodyContent);

        String actualBodyContent = this.message.getBodyAs(String.class);
        Assert.assertEquals(bodyContent, actualBodyContent);
    }

    @Test
    public void shouldReturnInteger() {
        int bodyContent = 123456;
        this.message.body("" + bodyContent);

        int actualBodyContent = this.message.getBodyAs(Integer.class);
        Assert.assertEquals(bodyContent, actualBodyContent);
    }

    @Test
    public void shouldReturnLong() {
        long bodyContent = 12345678901234l;
        this.message.body("" + bodyContent);

        long actualBodyContent = this.message.getBodyAs(Long.class);
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

        public void handleMessage(final Message message) {
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
