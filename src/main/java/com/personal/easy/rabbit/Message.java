package com.personal.easy.rabbit;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.personal.easy.rabbit.publisher.DeliveryOptions;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

/**
 * A message encapsulates the delivery content of a delivery from a broker. It wraps the body and
 * the message properties and contains commodity methods to access those in an easier fashion.
 *
 */
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

    /**
     * Get message id, which is an identifier of a message in business perspecitve.
     * @return
     */
    public String getId(){
        return this.id;
    }

    /**
     * Set message id, please fill in the unique business key for each message
     * @param id
     * @return
     */
    public Message id(final String id){
        this.id = id;
        // set messageId as header of the message
        this.basicProperties = this.basicProperties.builder().messageId(id).build();
        return this;
    }

	/**
	 * Gets the basic message properties (e.g. content encoding)
	 * 
	 * @return The message properties
	 */
	public BasicProperties getBasicProperties() {
		return this.basicProperties;
	}

    /**
     * Gets the body content in its rare byte
     * representation.
     *
     * @return The body content as bytes
     */
    public byte[] getBodyContent() {
        return this.bodyContent;
    }

    /**
     * <p>Gets the message body in the representation of the
     * specified Java type.</p>
     *
     * <p>Examples:</p>
     * <p>Body as String: getBodyAs(String.class)</p>
     * <p>Body as Integer: getBodyAs(Integer.class)</p>
     * <p>Body as boolean: getBodyAs(Boolean.class)</p>
     *
     * @param type The desired Java type
     * @return The body a the given Java type.
     */
    public <T> T getBodyAs(final Class<T> type) {
        return this.messageReader.readBodyAs(type);
    }

    /**
     * Gets the exchange to which the message is published to.
     *
     * @return The exchange
     */
	public String getExchange() {
		return this.exchange;
	}

    /**
     * Gets the routing key used to delegate messages to
     * queues bound to the set exchange. If this binding
     * matches the given routing then the message will be
     * put into the bound queue.
     *
     * @return The routing key
     */
	public String getRoutingKey() {
		return this.routingKey;
	}

    /**
     * Gets the message's delivery tag which is an identifier
     * for a message on the broker.
     *
     * @return The delivery tag
     */
	public long getDeliveryTag() {
		return this.deliveryTag;
	}

    /**
     * Sets the exchange to which the message is published to.
     *
     * @param exchange The exchange name
     * @return The modified message
     */
    public Message exchange(final String exchange) {
        this.exchange = exchange;
        return this;
    }

    /**
     * Convenient method to send a message directly to the
     * given queue instead to an exchange. Sets the exchange
     * name to the default exchange name and sets the routing
     * key to the given queue name.
     *
     * @param queue The queue name
     * @return The modified message
     */
    public Message queue(final String queue) {
        return exchange("").routingKey(queue);
    }

    /**
     * Sets the routing key used to delegate messages to
     * queues bound to the set exchange. If this binding
     * matches the given routing then the message will be
     * put into the bound queue.
     *
     * @param routingKey The routing key
     * @return The modified message
     */
    public Message routingKey(final String routingKey) {
        this.routingKey = routingKey;
        return this;
    }

	/**
	 * Adds the given body content to the message.
	 *
	 * @param bodyContent The body content as bytes
	 * @return The modified message
	 */
	public Message body(final byte[] bodyContent) {
		this.bodyContent = bodyContent;
		return this;
	}

    /**
     * Serializes and adds the given object as body to
     * the message using the default charset (UTF-8)
     * for encoding.
     *
     * @see MessageWriter#writeBody(Object)
     * @param body The message body object
     */
    public <T> Message body(final T body) {
        this.messageWriter.writeBody(body);
        return this;
    }

    /**
     * Serializes and adds the given object as body to
     * the message using the given charset for encoding.
     *
     * @see MessageWriter#writeBody(Object, Charset)
     */
    public <T> Message body(final T body, final Charset charset) {
        this.messageWriter.writeBody(body, charset);
        return this;
    }

	/**
	 * <p>
	 * Flags the message to be a persistent message. A persistent message survives a total broker failure as it is
     * persisted to disc if not already delivered to and acknowledged by all consumers.
	 * </p>
	 * 
	 * <p>
	 * Important: This flag only has affect if the queues on the broker are flagged as durable.
	 * </p>
	 * 
	 * @return The modified message
	 */
	public Message persistent() {
        this.basicProperties = this.basicProperties.builder()
                .deliveryMode(DELIVERY_MODE_PERSISTENT)
                .build();
		return this;
	}

	/**
	 * <p>
	 * Sets the delivery tag for this message.
	 * </p>
	 * 
	 * <p>
	 * Note: The delivery tag is generated automatically if not set manually.
     * </p>
	 * 
	 * @param deliveryTag The delivery tag
	 * @return The modified message
	 */
	public Message deliveryTag(final long deliveryTag) {
		this.deliveryTag = deliveryTag;
		return this;
	}

    /**
     * Sets the content charset encoding of this message.
     *
     * @param charset The charset encoding
     * @return The modified message
     */
    public Message contentEncoding(final String charset) {
        this.basicProperties = this.basicProperties.builder()
                .contentEncoding(charset)
                .build();
        return this;
    }

    /**
     * Sets the content type of this message.
     *
     * @param contentType The content type
     * @return The modified message
     */
    public Message contentType(final String contentType) {
        this.basicProperties = this.basicProperties.builder()
                .contentType(contentType)
                .build();
        return this;
    }

    /**
     * Publishes a message via the given channel.
     *
     * @param channel The channel used to publish the message
     * @throws IOException
     */
    public void publish(final Channel channel) throws IOException{
        publish(channel, DeliveryOptions.NONE);
    }

    /**
     * Publishes a message via the given channel while using the specified delivery options.
     *
     * @param channel The channel used to publish the message on
     * @param deliveryOptions The delivery options to use
     * @throws IOException
     */
    public void publish(final Channel channel, final DeliveryOptions deliveryOptions) throws IOException {
        // Assure to have a timestamp
        if (this.basicProperties.getTimestamp() == null) {
            this.basicProperties = this.basicProperties.builder().timestamp(new Date()).build();
        }

        boolean mandatory = deliveryOptions == DeliveryOptions.MANDATORY;
        boolean immediate = deliveryOptions == DeliveryOptions.IMMEDIATE;

        LOGGER.info("Publishing message {} to exchange '{}' with routing key '{}' (deliveryOptions: {}, persistent: {})",
                new Object[] { this.id, this.exchange, this.routingKey, deliveryOptions, this.basicProperties.getDeliveryMode() == 2 });

        channel.basicPublish(this.exchange, this.routingKey, mandatory, immediate, this.basicProperties, this.bodyContent);
        LOGGER.info("Successfully published message {} to exchange '{}' with routing key '{}'", this.id, this.exchange, this.routingKey);
    }

    /**
     * Publishes a message via the given channel, and waits for a confirmation that the message was
     * received by the broker.
     *
     * @param channel The channel used to publish the message
     * @throws IOException
     */
    public void publishAndWaitForConfirm(final Channel channel) throws IOException{
        publishAndWaitForConfirm(channel, DeliveryOptions.NONE);
    }

    /**
     * Publishes a message via the given channel while using the specified delivery options
     *    , and waits for a confirmation that the message was received by the broker.
     *
     * @param channel The channel used to publish the message on
     * @param deliveryOptions The delivery options to use
     * @throws IOException
     */
    public void publishAndWaitForConfirm(final Channel channel, final DeliveryOptions deliveryOptions) throws IOException {
        channel.confirmSelect();

        publish(channel, deliveryOptions);

        LOGGER.info("Waiting for publisher ack");

        try {
            channel.waitForConfirmsOrDie();
        } catch (InterruptedException e) {
            LOGGER.warn("Publishing message interrupted while waiting for producer ack", e);
            return;
        }

        LOGGER.info("Received publisher ack");
        return;
    }

}
