package com.personal.easy.rabbit.consumer;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.personal.easy.rabbit.message.Message;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * A simple implementation of a message consumer which implements rarely used
 * handles by logging them and providing a convenient method to handle delivered
 * messages.
 *
 * @author christian.bick
 * @author uwe.janner
 * @author soner.dastan
 *
 */
public class MessageConsumer extends ConsumerContainer.ManagedConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumer.class);

    private MessageCallback callback;

    public MessageConsumer() {
        super();
    }

    public MessageConsumer(final MessageCallback callback) {
        this.callback = callback;
    }

    public MessageCallback getCallback() {
        return this.callback;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleConsumeOk(final String consumerTag) {
        LOGGER.debug("Consumer {}: Received consume OK", consumerTag);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleCancelOk(final String consumerTag) {
        LOGGER.debug("Consumer {}: Received cancel OK", consumerTag);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleCancel(final String consumerTag) throws IOException {
        LOGGER.debug("Consumer {}: Received cancel", consumerTag);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleShutdownSignal(final String consumerTag, final ShutdownSignalException sig) {
        LOGGER.debug("Consumer {}: Received shutdown signal: {}", consumerTag, sig.getMessage());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleRecoverOk(final String consumerTag) {
        LOGGER.debug("Consumer {}: Received recover OK", consumerTag);
    }

    /**
     * <p>
     * Handles a message delivery from the broker by converting the received
     * message parts to a {@link com.personal.easy.rabbit.message.Message} which
     * provides convenient access to the message parts and hands it over to the
     * {@link com.personal.rabbitmq.consumer.MessageCallback.handleMessage(com.personal.
     * rabbitmq.Message)} method.
     * </p>
     *
     */
    @Override
    public void handleDelivery(final String consumerTag, final Envelope envelope, final BasicProperties properties,
            final byte[] body) throws IOException {
        LOGGER.debug("Consumer {}: Received handle delivery", consumerTag);
        Message message = new Message(properties).exchange(envelope.getExchange()).routingKey(envelope.getRoutingKey())
                .deliveryTag(envelope.getDeliveryTag()).body(body).id(properties.getMessageId());
        Object messageLogIdentifier = message.getId() == null ? message.getDeliveryTag() : message.getId();
        try {
            LOGGER.info("Consumer {}: Received message {}", consumerTag, messageLogIdentifier);
            this.callback.handleMessage(message);
        }
        catch (Throwable t) {
            if (!getConfiguration().isAutoAck()) {
                LOGGER.error("Consumer {}: Message {} could not be handled due to an exception during message processing",
                        new Object[] {
                                consumerTag, messageLogIdentifier, t
                });
                getChannel().basicNack(envelope.getDeliveryTag(), false, false);
                LOGGER.warn("Consumer {}: Nacked message {}", new Object[] {
                        consumerTag, messageLogIdentifier, t
                });
            }
            return;
        }
        if (!getConfiguration().isAutoAck()) {
            try {
                getChannel().basicAck(envelope.getDeliveryTag(), false);
                LOGGER.debug("Consumer {}: Acked message {}", consumerTag, messageLogIdentifier);
            }
            catch (IOException e) {
                LOGGER.error(
                        "Consumer {}: Message {} was processed but could not be acknowledged due to an exception when sending the acknowledgement",
                        new Object[] {
                                consumerTag, messageLogIdentifier, e
                        });
                throw e;
            }
        }
    }

}
