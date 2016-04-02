package com.personal.easy.rabbit.message;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.personal.easy.rabbit.publisher.DeliveryOptions;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

public class Message {

    private final static Logger LOGGER = LoggerFactory.getLogger(Message.class);

    public static final Charset DEFAULT_MESSAGE_CHARSET = Charset.forName("UTF-8");

    public static final int DELIVERY_MODE_PERSISTENT = 2;

    public static final String TEXT_PLAIN = "text/plain";

    public static final String APPLICATION_XML = "application/xml";

    public static final String APPLICATION_JSON = "application/json";

    private MessageReader messageReader;

    private MessageWriter messageWriter;

    private byte[] bodyContent = new byte[0];

    private BasicProperties basicProperties;

    private String routingKey = "";

    private String exchange = "";

    private long deliveryTag;

    private String id;

    public Message() {
        this(MessageProperties.PERSISTENT_BASIC);
    }

    public Message(final BasicProperties basicProperties) {
        this.basicProperties = basicProperties;
        this.messageReader = new MessageReader(this);
        this.messageWriter = new MessageWriter(this);
    }

    public String getId() {
        return this.id;
    }

    public Message id(final String id) {
        this.id = id;
        // set messageId as header of the message
        this.basicProperties = this.basicProperties.builder().messageId(id).build();
        return this;
    }

    public BasicProperties getBasicProperties() {
        return this.basicProperties;
    }

    public byte[] getBodyContent() {
        return this.bodyContent;
    }

    public <T> T getBodyAs(final Class<T> type) {
        return this.messageReader.readBodyAs(type);
    }

    public String getExchange() {
        return this.exchange;
    }

    public String getRoutingKey() {
        return this.routingKey;
    }

    public long getDeliveryTag() {
        return this.deliveryTag;
    }

    public Message exchange(final String exchange) {
        this.exchange = exchange;
        return this;
    }

    public Message queue(final String queue) {
        return exchange("").routingKey(queue);
    }

    public Message routingKey(final String routingKey) {
        this.routingKey = routingKey;
        return this;
    }

    public Message body(final byte[] bodyContent) {
        this.bodyContent = bodyContent;
        return this;
    }

    public <T> Message body(final T body) {
        this.messageWriter.writeBody(body);
        return this;
    }

    public <T> Message body(final T body, final Charset charset) {
        this.messageWriter.writeBody(body, charset);
        return this;
    }

    public Message persistent() {
        this.basicProperties = this.basicProperties.builder().deliveryMode(DELIVERY_MODE_PERSISTENT).build();
        return this;
    }

    public Message deliveryTag(final long deliveryTag) {
        this.deliveryTag = deliveryTag;
        return this;
    }

    public Message contentEncoding(final String charset) {
        this.basicProperties = this.basicProperties.builder().contentEncoding(charset).build();
        return this;
    }

    public Message contentType(final String contentType) {
        this.basicProperties = this.basicProperties.builder().contentType(contentType).build();
        return this;
    }

    public void publish(final Channel channel) throws IOException {
        publish(channel, DeliveryOptions.NONE);
    }

    public void publish(final Channel channel, final DeliveryOptions deliveryOptions) throws IOException {
        // Assure to have a timestamp
        if (this.basicProperties.getTimestamp() == null) {
            this.basicProperties = this.basicProperties.builder().timestamp(new Date()).build();
        }

        boolean mandatory = deliveryOptions == DeliveryOptions.MANDATORY;
        boolean immediate = deliveryOptions == DeliveryOptions.IMMEDIATE;

        LOGGER.info("Publishing message {} to exchange '{}' with routing key '{}' (deliveryOptions: {}, persistent: {})",
                new Object[] {
                        this.id, this.exchange, this.routingKey, deliveryOptions, this.basicProperties.getDeliveryMode() == 2
        });

        channel.basicPublish(this.exchange, this.routingKey, mandatory, immediate, this.basicProperties, this.bodyContent);
        LOGGER.info("Successfully published message {} to exchange '{}' with routing key '{}'", this.id, this.exchange,
                this.routingKey);
    }

    public void publishAndWaitForConfirm(final Channel channel) throws IOException {
        publishAndWaitForConfirm(channel, DeliveryOptions.NONE);
    }

    public void publishAndWaitForConfirm(final Channel channel, final DeliveryOptions deliveryOptions) throws IOException {
        channel.confirmSelect();

        publish(channel, deliveryOptions);

        LOGGER.info("Waiting for publisher ack");

        try {
            channel.waitForConfirmsOrDie();
        }
        catch (InterruptedException e) {
            LOGGER.warn("Publishing message interrupted while waiting for producer ack", e);
            return;
        }

        LOGGER.info("Received publisher ack");
        return;
    }

}
