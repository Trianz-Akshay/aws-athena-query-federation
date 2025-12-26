/*-
 * #%L
 * athena-hbase
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.hbase;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.DEFAULT_HBASE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HBASE_PORT;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.ZOOKEEPER_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HbaseEnvironmentPropertiesTest
{
    private static final String LOCALHOST = "localhost";
    private static final String PORT_2181 = "2181";
    private static final String PORT_2182 = "2182";
    private static final String HBASE_EXAMPLE_COM = "hbase.example.com";
    private static final String PORT_9090 = "9090";
    private static final int EXPECTED_ENVIRONMENT_SIZE = 1;

    @Test
    public void connectionPropertiesToEnvironment_withValidProperties_returnsEnvironmentMap()
    {
        HbaseEnvironmentProperties properties = new HbaseEnvironmentProperties();
        Map<String, String> connectionProperties = ImmutableMap.of(
                HOST, LOCALHOST,
                HBASE_PORT, PORT_2181,
                ZOOKEEPER_PORT, PORT_2182
        );

        Map<String, String> environment = properties.connectionPropertiesToEnvironment(connectionProperties);

        assertNotNull("Environment map should not be null", environment);
        assertEquals("Should have one environment variable", EXPECTED_ENVIRONMENT_SIZE, environment.size());
        assertEquals("Environment value should match expected format", "localhost:2181:2182", environment.get(DEFAULT_HBASE));
    }

    @Test
    public void connectionPropertiesToEnvironment_withDifferentProperties_returnsEnvironmentMap()
    {
        HbaseEnvironmentProperties properties = new HbaseEnvironmentProperties();
        Map<String, String> connectionProperties = ImmutableMap.of(
                HOST, HBASE_EXAMPLE_COM,
                HBASE_PORT, PORT_9090,
                ZOOKEEPER_PORT, PORT_2181
        );

        Map<String, String> environment = properties.connectionPropertiesToEnvironment(connectionProperties);

        assertNotNull("Environment map should not be null", environment);
        assertEquals("Environment value should match expected format", "hbase.example.com:9090:2181", environment.get(DEFAULT_HBASE));
    }
}

