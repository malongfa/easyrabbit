package com.personal.easy.rabbit.publisher;


import org.junit.After;
import org.junit.Before;

import com.personal.easy.rabbit.SingleConnectionFactory;
import com.personal.easy.rabbit.TestBrokerSetup;
import com.personal.easy.rabbit.setting.BrokerAssert;


public abstract class MessagePublisherIT {

    protected SingleConnectionFactory singleConnectionFactory;
    protected TestBrokerSetup brokerSetup;
    protected BrokerAssert brokerAssert;

    @Before
    public void beforeAll() throws Exception {
        brokerAssert = new BrokerAssert();
        brokerSetup = new TestBrokerSetup();
        brokerSetup.prepareSimpleTest();
        singleConnectionFactory = new SingleConnectionFactory();
        singleConnectionFactory.setHost(brokerSetup.getHost());
        singleConnectionFactory.setPort(brokerSetup.getPort());
    }

    @After
    public void after() throws Exception {
        brokerSetup.tearDown();
        singleConnectionFactory.close();
    }

}
