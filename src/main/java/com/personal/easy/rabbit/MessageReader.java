package com.personal.easy.rabbit;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;

import com.alibaba.fastjson.JSON;
import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * Provides utilities for an easy access of message content.
 *
 */
public class MessageReader {

    private Message message;

    public MessageReader(final Message message) {
        this.message = message;
    }

    /**
     * Extracts the message body's charset encoding by looking it up in the
     * message properties if given there and choosing the default charset
     * (UTF-8) otherwise.
     *
     * @return The message body's charset encoding
     */
    public Charset readCharset() {
        BasicProperties basicProperties = this.message.getBasicProperties();
        if (basicProperties == null) {
            return Message.DEFAULT_MESSAGE_CHARSET;
        }
        String contentCharset = basicProperties.getContentEncoding();
        if (contentCharset == null) {
            return Message.DEFAULT_MESSAGE_CHARSET;
        }
        return Charset.forName(contentCharset);
    }

    /**
     * Extracts the message body and interprets it as the given Java type
     *
     * @param type
     *            The Java type
     * @return The message body a the specified type
     */
    @SuppressWarnings("unchecked")
    public <T> T readBodyAs(final Class<T> type) {
        if (String.class.isAssignableFrom(type)) {
            return (T) readBodyAsString();
        } else if (Number.class.isAssignableFrom(type)) {
            return (T) readBodyAsNumber((Class<Number>) type);
        } else if (Boolean.class.isAssignableFrom(type)) {
            return (T) readBodyAsBoolean();
        } else if (Character.class.isAssignableFrom(type)) {
            return (T) readBodyAsChar();
        }
        return readBodyAsObject(type);
    }

    /**
     * Extracts the message body and interprets it as a string.
     *
     * @return The message body as string
     */
    public String readBodyAsString() {
        Charset charset = readCharset();
        byte[] bodyContent = this.message.getBodyContent();
        return new String(bodyContent, charset);
    }

    /**
     * Extracts the message body and interprets it as a boolean.
     *
     * @return The message body as boolean
     */
    public Boolean readBodyAsBoolean() {
        String messageContent = readBodyAsString();
        return Boolean.valueOf(messageContent);
    }

    /**
     * Extracts the message body and interprets it as a single character.
     *
     * @return Tje message body as character
     */
    public Character readBodyAsChar() {
        String messageContent = readBodyAsString();
        return messageContent.charAt(0);
    }

    /**
     * Extracts the message body and interprets it as the given number (e.g.
     * Integer) .
     *
     * @param type
     *            The number type
     * @return The message body as the specified number
     */
    @SuppressWarnings("unchecked")
    public <T extends Number> T readBodyAsNumber(final Class<T> type) {
        String messageContent = readBodyAndValidateForNumber();
        if (type.equals(BigDecimal.class)) {
            return (T) new BigDecimal(messageContent);
        } else if (type.equals(BigInteger.class)) {
            return (T) new BigInteger(messageContent);
        } else if (type.equals(Byte.class)) {
            return (T) Byte.valueOf(messageContent);
        } else if (type.equals(Short.class)) {
            return (T) Short.valueOf(messageContent);
        } else if (type.equals(Integer.class)) {
            return (T) Integer.valueOf(messageContent);
        } else if (type.equals(Long.class)) {
            return (T) Long.valueOf(messageContent);
        } else {
            throw new RuntimeException("Unsupported number format: " + type);
        }
    }

    /**
     * Extracts the message body and interprets it as the XML representation of
     * an object of the given type.
     *
     * @param type
     *            The type (class) of the object
     * @return The message body as an object of the specified type
     */

    public <T> T readBodyAsObject(final Class<T> type) {

        String ret = new String(this.message.getBodyContent());
        return JSON.parseObject(ret, type);

    }

    /**
     * Extracts the charset of the message content. If no charset is provided in
     * the message properties then the default charset (UTF-8) is used.
     *
     * @return The message content charset
     */
    String readBodyAndValidateForNumber() {
        String messageContent = readBodyAsString();
        if (messageContent == null || messageContent.isEmpty()) {
            throw new RuntimeException("Message is empty");
        }
        // Check if content is a number
        for (int i = 0; i < messageContent.length(); i++) {
            if (!Character.isDigit(messageContent.charAt(i))) {
                throw new RuntimeException("Message is not a number");
            }
        }
        return messageContent;
    }

}
