/*
 * Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.boo.awsmqtt.pubSub;

import com.amazonaws.services.iot.client.*;
import com.boo.awsmqtt.sampleUtil.SampleUtil;
import com.boo.awsmqtt.sampleUtil.SampleUtil.KeyStorePasswordPair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * This is an example that uses {@link AWSIotMqttClient} to subscribe to a topic and
 * publish messages to it. Both blocking and non-blocking publishing are
 * demonstrated in this example.
 */
@Component
public class PublishSubscribeSample {

    private static final String TEMPERATURE_TOPIC = "outgoing";
    private static final AWSIotQos TestTopicQos = AWSIotQos.QOS0;
    private static AWSIotMqttClient awsIotClient;
    @Value("${clientEndpoint}")
    private String clientEndpoint;
    @Value("${clientId}")
    private String clientId;
    @Value("${certificateFile}")
    private String certificateFile;
    @Value("${privateKeyFile}")
    private String privateKeyFile;

    @PostConstruct
    public void init() throws InterruptedException, AWSIotException {

        if (awsIotClient == null && certificateFile != null && privateKeyFile != null) {
            KeyStorePasswordPair pair = SampleUtil.getKeyStorePasswordPair(certificateFile, privateKeyFile, null);
            awsIotClient = new AWSIotMqttClient(clientEndpoint, clientId, pair.keyStore, pair.keyPassword);
        }

        if (awsIotClient == null) {
            throw new IllegalArgumentException("Failed to construct client due to missing certificate or credentials.");
        }
        awsIotClient.connect();

        AWSIotTopic topic = new MyTopicListener(TEMPERATURE_TOPIC, TestTopicQos);
        awsIotClient.subscribe(topic, true);

        Thread nonBlockingPublishThread = new Thread(new NonBlockingPublisher(awsIotClient));
        nonBlockingPublishThread.start();
        nonBlockingPublishThread.join();
    }

    public static class NonBlockingPublisher implements Runnable {
        private final AWSIotMqttClient awsIotClient;

        public NonBlockingPublisher(AWSIotMqttClient awsIotClient) {
            this.awsIotClient = awsIotClient;
        }

        @Override
        public void run() {
            long counter = 1;

            while (true) {
                String payload = "hello from non-blocking publisher - " + (counter++);
                AWSIotMessage message = new NonBlockingPublishListener(TEMPERATURE_TOPIC, TestTopicQos, payload);
                try {
                    awsIotClient.publish(message);
                } catch (AWSIotException e) {
                    System.out.println(System.currentTimeMillis() + ": publish failed for " + payload);
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    System.out.println(System.currentTimeMillis() + ": NonBlockingPublisher was interrupted");
                    return;
                }
            }
        }
    }

}
