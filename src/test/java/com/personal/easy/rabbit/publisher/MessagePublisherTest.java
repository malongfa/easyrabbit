package com.personal.easy.rabbit.publisher;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.easymock.EasyMock.expect;

@RunWith(PowerMockRunner.class)
public abstract class MessagePublisherTest {

	public static final String TEST_EXCHANGE = "lib.test.exchange";
	public static final String TEST_ROUTING_KEY = "lib.test.routingKey";
	@Mock
	protected ConnectionFactory connectionFactory;
	@Mock
	protected Connection connection;
	@Mock
	protected Channel channel;

	protected void mockConnectionOperations() throws Exception {
		expect(connectionFactory.newConnection()).andReturn(connection).anyTimes();
		expect(connection.createChannel()).andReturn(channel).anyTimes();
		expect(channel.isOpen()).andReturn(false).anyTimes();
	}

}
