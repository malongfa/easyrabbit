package com.personal.easy.rabbit;

import java.util.concurrent.TimeoutException;

import com.personal.easy.rabbit.ConnectionListener;
import com.personal.easy.rabbit.SingleConnectionFactory;
import com.personal.easy.rabbit.setting.BrokerConnection;
import com.rabbitmq.client.Connection;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SingleConnectionFactoryIT {

    SingleConnectionFactory singleConnectionFactory;
    
    @Before
    public void before() {
        singleConnectionFactory = new SingleConnectionFactory();
        singleConnectionFactory.setHost(BrokerConnection.getDefaultHost());
        singleConnectionFactory.setPort(BrokerConnection.getDefaultPort());
    }
    
    @After
    public void after() throws TimeoutException {
        singleConnectionFactory.close();
    }
    
    @Test
    public void shouldNotifyAboutConnectionState() throws Exception {
        TestConnectionListener connectionListener = new TestConnectionListener();
        singleConnectionFactory.registerListener(connectionListener);
        Connection connection = singleConnectionFactory.newConnection();
        Assert.assertNotNull(connection);
        Assert.assertTrue(connection.isOpen());
        Assert.assertTrue(connectionListener.connectionEstablishedTriggered);
        connection.close();
        // Have a short sleep because connection closed notification happens asynchronously
        Thread.sleep(50);
        Assert.assertTrue(connectionListener.connectionLostTriggered);
        connection = singleConnectionFactory.newConnection();
        Assert.assertNotNull(connection);
        Assert.assertTrue(connectionListener.connectionEstablishedTriggered);
        Assert.assertTrue(connection.isOpen());
        singleConnectionFactory.close();
        Assert.assertTrue(connectionListener.connectionClosedTriggered);
    }

    @Test
    public void shouldReturnSameConnection() throws Exception {
        Connection connectionOne = singleConnectionFactory.newConnection();
        Connection connectionTwo = singleConnectionFactory.newConnection();
        Assert.assertTrue(connectionOne == connectionTwo);
    }
    
    @Test
    public void shouldReconnect() throws Exception {
        singleConnectionFactory.newConnection();
        Assert.assertNotNull(singleConnectionFactory.connection);
        Assert.assertTrue(singleConnectionFactory.connection.isOpen());
        singleConnectionFactory.setPort(15345);
        singleConnectionFactory.connection.close();
        int waitForReconnects = SingleConnectionFactory.CONNECTION_ESTABLISH_INTERVAL_IN_MS + SingleConnectionFactory.CONNECTION_TIMEOUT_IN_MS  * 2;
        Thread.sleep(waitForReconnects);
        singleConnectionFactory.setPort(BrokerConnection.getDefaultPort());
        Thread.sleep(waitForReconnects);
        Assert.assertNotNull(singleConnectionFactory.connection);
        Assert.assertTrue(singleConnectionFactory.connection.isOpen());
    }
    
    private static class TestConnectionListener implements ConnectionListener {

        volatile boolean connectionEstablishedTriggered;
        volatile boolean connectionLostTriggered;
        volatile boolean connectionClosedTriggered;

        public void onConnectionEstablished(Connection connection) {
            connectionEstablishedTriggered = true;
        }

       
        public void onConnectionLost(Connection connection) {
            connectionLostTriggered = true;
        }

      
        public void onConnectionClosed(Connection connection) {
            connectionClosedTriggered = true;
        }
    }
}
