/*-
 * #%L
 * athena-kafka
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;



@RunWith(MockitoJUnitRunner.class)
public class KafkaCompositeHandlerTest {

    static {
        System.setProperty("aws.region", "us-west-2");
    }

    private java.util.Map<String, String> configOptions = com.google.common.collect.ImmutableMap.of(
        "glue_registry_arn", "arn:aws:glue:us-west-2:123456789101:registry/Athena-Kafka",
        "bootstrap.servers", "test"
    );

    @Mock
    KafkaConsumer<String, String> kafkaConsumer;

    private KafkaCompositeHandler kafkaCompositeHandler;


    @Mock
    private SecretsManagerClient secretsManager;

    private MockedStatic<KafkaUtils> mockedKafkaUtils;
    private MockedStatic<SecretsManagerClient> mockedSecretsManagerClient;
    @Before
    public void setUp() throws Exception {
        mockedSecretsManagerClient = Mockito.mockStatic(SecretsManagerClient.class);
        mockedSecretsManagerClient.when(()-> SecretsManagerClient.create()).thenReturn(secretsManager);
        mockedKafkaUtils = Mockito.mockStatic(KafkaUtils.class);
        mockedKafkaUtils.when(() -> KafkaUtils.getKafkaConsumer(configOptions)).thenReturn(kafkaConsumer);
    }

    @After
    public void close() {
        mockedKafkaUtils.close();
        mockedSecretsManagerClient.close();
    }

    @Test
    public void kafkaCompositeHandlerTest() throws Exception {

        kafkaCompositeHandler = new KafkaCompositeHandler();
        Assert.assertTrue(kafkaCompositeHandler instanceof KafkaCompositeHandler);
    }

}
