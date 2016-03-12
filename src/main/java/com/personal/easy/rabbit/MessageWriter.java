package com.personal.easy.rabbit;

import java.nio.charset.Charset;

import com.alibaba.fastjson.JSON;

public class MessageWriter {

    private Message message;

    public MessageWriter(final Message message) {
        this.message = message;
    }

    /**
     * <p>
     * Writes the message body using the given object of type T using the
     * default charset UTF-8.
     * </p>
     *
     * @see #writeBody(Object,Charset)
     * @param body
     *            The message body
     * @param <T>
     *            The object type
     */
    public <T> void writeBody(final T body) {
        writeBody(body, Message.DEFAULT_MESSAGE_CHARSET);
    }

    /**
     * <p>
     * Writes the message body using the given object of type T using the given
     * charset for character encoding.
     * </p>
     *
     * <p>
     * For primitive types and Strings, the message body is written as plain
     * text. In all other cases, the object is serialized to XML.
     * </p>
     *
     * @param body
     *            The message body
     * @param charset
     *            The charset to use for character encoding
     * @param <T>
     *            The object type
     */
    public <T> void writeBody(final T body, final Charset charset) {
        if (isPrimitive(body)) {
            String bodyAsString = String.valueOf(body);
            writeBodyFromString(bodyAsString, charset);
        } else if (isString(body)) {
            String bodyAsString = (String) body;
            writeBodyFromString(bodyAsString, charset);
        } else {
            writeBodyFromObject(body, charset);
        }
    }

    /**
     * Writes the message body from a string using the given charset as encoding
     * and setting the content type to text/plain.
     *
     * @param bodyAsString
     *            Body to write as string
     * @param charset
     *            The charset to encode the string
     */
    public void writeBodyFromString(final String bodyAsString, final Charset charset) {
        this.message.contentEncoding(charset.name()).contentType(Message.TEXT_PLAIN);
        byte[] bodyContent = bodyAsString.getBytes(charset);
        this.message.body(bodyContent);
    }

    /**
     * Writes the body by serializing the given object to XML.
     *
     * @param bodyAsObject
     *            The body as object
     * @param <T>
     *            The object type
     */
    public <T> void writeBodyFromObject(final T bodyAsObject, final Charset charset) {

        String result = JSON.toJSONString(bodyAsObject);

        byte[] bodyContent = result.getBytes();
        this.message.contentType(Message.APPLICATION_JSON).contentEncoding(
                charset.name());
        this.message.body(bodyContent);
    }

    boolean isString(final Object object) {
        return object instanceof String;
    }

    boolean isPrimitive(final Object object) {
        return object.getClass().isPrimitive() || object instanceof Boolean
                || object instanceof Character || object instanceof Number;
    }

    String unCapitalizedClassName(final Class<?> clazz) {
        String className = clazz.getSimpleName();
        return Character.toLowerCase(className.charAt(0))
                + className.substring(1);
    }

}
