package com.personal.easy.rabbit.setting;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * A broker connection provides one single connection for each broker involved
 * in a test.
 *
 */
public class BrokerConnection {

	public static final String PROPERTY_HOST = "rabbiteasy.test.host";
	public static final String PROPERTY_PORT = "rabbiteasy.test.port";

	private static Map<String, Connection> connections = new HashMap<String, Connection>();

	public static String getDefaultHost() {
		return System.getProperty(PROPERTY_HOST) == null ? ConnectionFactory.DEFAULT_HOST
				: System.getProperty(PROPERTY_HOST);
	}

	public static int getDefaultPort() {
		return System.getProperty(PROPERTY_PORT) == null ? ConnectionFactory.DEFAULT_AMQP_PORT
				: Integer.valueOf(System.getProperty(PROPERTY_PORT));
	}

	public static Connection getConnection() {
		String host = getDefaultHost();
		int port = getDefaultPort();
		return getConnection(host, port);
	}

	public static synchronized Connection getConnection(String host, int port) {
		String factoryKey = host + ":" + port;
		try {
			Connection connection = connections.get(factoryKey);
			if (connection == null || !connection.isOpen()) {
				ConnectionFactory connectionFactory = new ConnectionFactory();
				connectionFactory.setHost(host);
				connectionFactory.setPort(port);
				connections.put(factoryKey, connectionFactory.newConnection());
			}
			return connections.get(factoryKey);
		} catch (IOException e) {
			throw new RuntimeException("Failed to create a new connection for "
					+ factoryKey, e);
		} catch (TimeoutException e) {
			throw new RuntimeException("Failed to create a new connection for "
					+ factoryKey, e);
		}
	}

}
