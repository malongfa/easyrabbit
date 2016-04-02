package com.personal.easy.rabbit.connection;

import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.personal.easy.rabbit.setting.BrokerConnection;
import com.rabbitmq.client.Connection;

import junit.framework.Assert;

public class SingleConnectionFactoryIT {

    SingleConnectionFactory singleConnectionFactory;
    
    @Before
    public void before() {
        this.singleConnectionFactory = new SingleConnectionFactory();
        this.singleConnectionFactory.setHost(BrokerConnection.getDefaultHost());
        this.singleConnectionFactory.setPort(BrokerConnection.getDefaultPort());
    }
    
    @After
    public void after() throws TimeoutException {
        this.singleConnectionFactory.close();
    }
    
    @Test
    public void shouldNotifyAboutConnectionState() throws Exception {
        TestConnectionListener connectionListener = new TestConnectionListener();
        this.singleConnectionFactory.registerListener(connectionListener);
        Connection connection = this.singleConnectionFactory.newConnection();
        Assert.assertNotNull(connection);
        Assert.assertTrue(connection.isOpen());
        Assert.assertTrue(connectionListener.connectionEstablishedTriggered);
        connection.close();
        Thread.sleep(50);
        Assert.assertTrue(connectionListener.connectionLostTriggered);
        connection = this.singleConnectionFactory.newConnection();
        Assert.assertNotNull(connection);
        Assert.assertTrue(connectionListener.connectionEstablishedTriggered);
        Assert.assertTrue(connection.isOpen());
        this.singleConnectionFactory.close();
        Assert.assertTrue(connectionListener.connectionClosedTriggered);
    }

    @Test
    public void shouldReturnSameConnection() throws Exception {
        Connection connectionOne = this.singleConnectionFactory.newConnection();
        Connection connectionTwo = this.singleConnectionFactory.newConnection();
        Assert.assertTrue(connectionOne == connectionTwo);
    }
    
    @Test
    public void shouldReconnect() throws Exception {
        this.singleConnectionFactory.newConnection();
        Assert.assertNotNull(this.singleConnectionFactory.connection);
        Assert.assertTrue(this.singleConnectionFactory.connection.isOpen());
        this.singleConnectionFactory.setPort(15345);
        this.singleConnectionFactory.connection.close();
        int waitForReconnects = SingleConnectionFactory.CONNECTION_ESTABLISH_INTERVAL_IN_MS + SingleConnectionFactory.CONNECTION_TIMEOUT_IN_MS  * 2;
        Thread.sleep(waitForReconnects);
        this.singleConnectionFactory.setPort(BrokerConnection.getDefaultPort());
        Thread.sleep(waitForReconnects);
        Assert.assertNotNull(this.singleConnectionFactory.connection);
        Assert.assertTrue(this.singleConnectionFactory.connection.isOpen());
    }
    
    private static class TestConnectionListener implements ConnectionListener {

        volatile boolean connectionEstablishedTriggered;
        volatile boolean connectionLostTriggered;
        volatile boolean connectionClosedTriggered;

        @Override
        public void onConnectionEstablished(final Connection connection) {
            this.connectionEstablishedTriggered = true;
        }

       
        @Override
        public void onConnectionLost(final Connection connection) {
            this.connectionLostTriggered = true;
        }

      
        @Override
        public void onConnectionClosed(final Connection connection) {
            this.connectionClosedTriggered = true;
        }
    }
}
