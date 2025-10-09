/*-
 * #%L
 * athena-neptune
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
package com.amazonaws.athena.connectors.neptune;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.CLUSTER_RES_ID;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.GRAPH_TYPE;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.HOST;
import static com.amazonaws.athena.connector.lambda.connection.EnvironmentConstants.PORT;
import static com.amazonaws.athena.connectors.neptune.Constants.CFG_ClUSTER_RES_ID;
import static com.amazonaws.athena.connectors.neptune.Constants.CFG_ENDPOINT;
import static com.amazonaws.athena.connectors.neptune.Constants.CFG_GRAPH_TYPE;
import static com.amazonaws.athena.connectors.neptune.Constants.CFG_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(MockitoJUnitRunner.class)
public class NeptuneEnvironmentPropertiesTest {

    @Test
    public void testConnectionPropertiesToEnvironment() {
        // Create input connection properties
        Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, "test-endpoint");
        connectionProperties.put(PORT, "8182");
        connectionProperties.put(CFG_ClUSTER_RES_ID, "test-cluster-id");
        connectionProperties.put(GRAPH_TYPE, "propertygraph");

        // Create instance and call method
        NeptuneEnvironmentProperties properties = new NeptuneEnvironmentProperties();
        Map<String, String> result = properties.connectionPropertiesToEnvironment(connectionProperties);

        // Verify results
        assertNotNull("Result should not be null", result);
        assertEquals("Endpoint should match", "test-endpoint", result.get(CFG_ENDPOINT));
        assertEquals("Port should match", "8182", result.get(CFG_PORT));
    }

    @Test
    public void testConnectionPropertiesToEnvironment_WithNullValues() {
        // Create input connection properties with null values
        Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put(HOST, null);
        connectionProperties.put(PORT, null);
        connectionProperties.put(CLUSTER_RES_ID, null);
        connectionProperties.put(GRAPH_TYPE, null);

        // Create environment map with null values
        Map<String, String> environment = new HashMap<>();
        environment.put(CLUSTER_RES_ID, null);
        environment.put(GRAPH_TYPE, null);

        // Create instance and call method
        NeptuneEnvironmentProperties properties = new NeptuneEnvironmentProperties();
        Map<String, String> result = properties.connectionPropertiesToEnvironment(connectionProperties);

        // Verify results
        assertNotNull("Result should not be null", result);
        assertNull("Endpoint should be null", result.get(CFG_ENDPOINT));
        assertNull("Port should be null", result.get(CFG_PORT));
        assertEquals("Cluster resource ID should be null", environment.get(CLUSTER_RES_ID), result.get(CFG_ClUSTER_RES_ID));
        assertEquals("Graph type should be null", environment.get(GRAPH_TYPE), result.get(CFG_GRAPH_TYPE));
    }
} 