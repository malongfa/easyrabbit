package com.personal.easy.rabbit.message;

import java.nio.charset.Charset;

import com.alibaba.fastjson.JSON;

public class MessageWriter {

    private Message message;

    public MessageWriter(final Message message) {
        this.message = message;
    }

    public <T> void writeBody(final T body) {
        writeBody(body, Message.DEFAULT_MESSAGE_CHARSET);
    }

    public <T> void writeBody(final T body, final Charset charset) {
        if (isPrimitive(body)) {
            String bodyAsString = String.valueOf(body);
            writeBodyFromString(bodyAsString, charset);
        }
        else if (isString(body)) {
            String bodyAsString = (String) body;
            writeBodyFromString(bodyAsString, charset);
        }
        else {
            writeBodyFromObject(body, charset);
        }
    }

    public void writeBodyFromString(final String bodyAsString, final Charset charset) {
        this.message.contentEncoding(charset.name()).contentType(Message.TEXT_PLAIN);
        byte[] bodyContent = bodyAsString.getBytes(charset);
        this.message.body(bodyContent);
    }

    public <T> void writeBodyFromObject(final T bodyAsObject, final Charset charset) {

        String result = JSON.toJSONString(bodyAsObject);

        byte[] bodyContent = result.getBytes();
        this.message.contentType(Message.APPLICATION_JSON).contentEncoding(charset.name());
        this.message.body(bodyContent);
    }

    boolean isString(final Object object) {
        return object instanceof String;
    }

    boolean isPrimitive(final Object object) {
        return object.getClass().isPrimitive() || object instanceof Boolean || object instanceof Character
                || object instanceof Number;
    }

    String unCapitalizedClassName(final Class<?> clazz) {
        String className = clazz.getSimpleName();
        return Character.toLowerCase(className.charAt(0)) + className.substring(1);
    }

}
