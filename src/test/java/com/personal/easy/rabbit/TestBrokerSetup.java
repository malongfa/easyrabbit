package com.personal.easy.rabbit;

import com.personal.easy.rabbit.setting.BrokerSetup;




public class TestBrokerSetup extends BrokerSetup {
    
    public static final String TEST_EXCHANGE = "lib.test.exchange";
    public static final String TEST_ROUTING_KEY = "lib.test.routing.key";
    public static final String TEST_QUEUE = "lib.test.queue";
    public static final String TEST_QUEUE_DEAD = TEST_QUEUE + ":dead";
    public static final String TEST_HA_QUEUE = "lib.test.ha.queue";

    public TestBrokerSetup() {
        super();
    }
    
    public void prepareSimpleTest() throws Exception {
        declareExchange(TestBrokerSetup.TEST_EXCHANGE, "topic");
        declareAndBindQueue(
            TestBrokerSetup.TEST_QUEUE, TestBrokerSetup.TEST_EXCHANGE, TestBrokerSetup.TEST_ROUTING_KEY);
    }
    

    public void prepareHighAvailabilityTest() throws Exception {
        declareExchange(TestBrokerSetup.TEST_EXCHANGE, "topic");
        declareAndBindQueueWithHighAvailability(
            TestBrokerSetup.TEST_HA_QUEUE, TestBrokerSetup.TEST_EXCHANGE, TestBrokerSetup.TEST_ROUTING_KEY);
    }
    
}
