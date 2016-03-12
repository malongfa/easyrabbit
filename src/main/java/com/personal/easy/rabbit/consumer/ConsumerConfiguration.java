package com.personal.easy.rabbit.consumer;

/**
 * A consumer configuration holds parameters to be set before enabling a consumer to
 * consume messages from the message broker.
 *
 */
public class ConsumerConfiguration {

    public static final int UNLIMITED_PREFETCH_MESSAGE_COUNT = 5;

	private String queueName;
	private boolean autoAck = false;
    private int prefetchMessageCount = UNLIMITED_PREFETCH_MESSAGE_COUNT;

	public ConsumerConfiguration(String queueName) {
		this.queueName = queueName;
	}

    public ConsumerConfiguration(String queueName, boolean autoAck) {
        this.queueName = queueName;
        this.autoAck = autoAck;
    }

    public ConsumerConfiguration(String queueName, int prefetchMessageCount) {
        this.queueName = queueName;
        this.prefetchMessageCount = prefetchMessageCount;
    }

    public ConsumerConfiguration(String queueName, boolean autoAck, int prefetchMessageCount) {
        this.queueName = queueName;
        this.autoAck = autoAck;
        this.prefetchMessageCount = prefetchMessageCount;
    }

	public String getQueueName() {
		return queueName;
	}

	public boolean isAutoAck() {
		return autoAck;
	}

    public int getPrefetchMessageCount() {
        return prefetchMessageCount;
    }
}
