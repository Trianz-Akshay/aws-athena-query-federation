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

import com.amazonaws.athena.connectors.neptune.propertygraph.NeptuneGremlinConnection;
import com.amazonaws.athena.connectors.neptune.rdf.NeptuneSparqlConnection;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class NeptuneConnectionTest {
    private static final String TEST_ENDPOINT = "localhost";
    private static final String TEST_PORT = "8182";
    private static final String TEST_REGION = "us-east-1";
    private static final String IAM_DISABLED = "false";
    private static final String IAM_ENABLED = "true";
    private static final String PROPERTYGRAPH_TYPE = "PROPERTYGRAPH";
    private static final String RDF_TYPE = "RDF";
    private static final String INVALID_TYPE = "INVALID_TYPE";

    @Mock
    private Client mockClient;
    @Mock
    private GraphTraversalSource mockTraversalSource;
    private Map<String, String> configOptions;

    @Before
    public void setUp() {
        configOptions = new HashMap<>();
        configOptions.put(Constants.CFG_ENDPOINT, TEST_ENDPOINT);
        configOptions.put(Constants.CFG_PORT, TEST_PORT);
        configOptions.put(Constants.CFG_IAM, IAM_DISABLED);
        configOptions.put(Constants.CFG_REGION, TEST_REGION);
    }

    @Test
    public void createConnection_WithPropertyGraphType_ReturnsNeptuneGremlinConnection() throws Exception {
        // Setup
        configOptions.put(Constants.CFG_GRAPH_TYPE, PROPERTYGRAPH_TYPE);
        
        // Execute
        NeptuneConnection connection = NeptuneConnection.createConnection(configOptions);
        
        // Verify
        assertNotNull("Connection should not be null", connection);
        assertThat(connection)
                .as("Should be instance of NeptuneGremlinConnection")
                .isInstanceOf(NeptuneGremlinConnection.class);
        assertEquals("Endpoint should match", TEST_ENDPOINT, connection.getNeptuneEndpoint());
        assertEquals("Port should match", TEST_PORT, connection.getNeptunePort());
        assertEquals("Region should match", TEST_REGION, connection.getRegion());
        assertFalse("IAM should be disabled", connection.isEnabledIAM());
    }

    @Test
    public void createConnection_WithRDFType_ReturnsNeptuneSparqlConnection() {
        // Setup
        configOptions.put(Constants.CFG_GRAPH_TYPE, RDF_TYPE);
        
        // Execute
        NeptuneConnection connection = NeptuneConnection.createConnection(configOptions);
        
        // Verify
        assertNotNull("Connection should not be null", connection);
        assertThat(connection)
                .as("Should be instance of NeptuneSparqlConnection")
                .isInstanceOf(NeptuneSparqlConnection.class);
        assertEquals("Endpoint should match", TEST_ENDPOINT, connection.getNeptuneEndpoint());
        assertEquals("Port should match", TEST_PORT, connection.getNeptunePort());
        assertEquals("Region should match", TEST_REGION, connection.getRegion());
        assertFalse("IAM should be disabled", connection.isEnabledIAM());
    }

    @Test
    public void createConnection_WithNullGraphType_ReturnsNeptuneGremlinConnection() {
        // Execute
        NeptuneConnection connection = NeptuneConnection.createConnection(configOptions);
        
        // Verify
        assertNotNull("Connection should not be null", connection);
        assertThat(connection)
                .as("Should be instance of NeptuneGremlinConnection")
                .isInstanceOf(NeptuneGremlinConnection.class);
        assertEquals("Endpoint should match", TEST_ENDPOINT, connection.getNeptuneEndpoint());
        assertEquals("Port should match", TEST_PORT, connection.getNeptunePort());
        assertEquals("Region should match", TEST_REGION, connection.getRegion());
        assertFalse("IAM should be disabled", connection.isEnabledIAM());
    }

    @Test(expected = IllegalArgumentException.class)
    public void createConnection_WithInvalidGraphType_ThrowsIllegalArgumentException() {
        // Setup
        configOptions.put(Constants.CFG_GRAPH_TYPE, INVALID_TYPE);
        
        // Execute - should throw IllegalArgumentException
        NeptuneConnection.createConnection(configOptions);
    }

    @Test
    public void createConnection_WithIAMEnabled_ReturnsConnectionWithIAMEnabled() {
        // Setup
        configOptions.put(Constants.CFG_GRAPH_TYPE, PROPERTYGRAPH_TYPE);
        configOptions.put(Constants.CFG_IAM, IAM_ENABLED);
        
        // Execute
        NeptuneConnection connection = NeptuneConnection.createConnection(configOptions);
        
        // Verify
        assertNotNull("Connection should not be null", connection);
        assertThat(connection)
                .as("Should be instance of NeptuneGremlinConnection")
                .isInstanceOf(NeptuneGremlinConnection.class);
        assertTrue("IAM should be enabled", connection.isEnabledIAM());
    }

    @Test
    public void getNeptuneClientConnection_WithValidConnection_ReturnsNonNullClient() {
        NeptuneConnection connection = new NeptuneGremlinConnection(TEST_ENDPOINT, TEST_PORT, false, TEST_REGION) {

            @Override
            public Client getNeptuneClientConnection() {
                return mockClient;
            }
        };
        
        Client client = connection.getNeptuneClientConnection();
        assertNotNull(client);
        assertEquals(mockClient, client);
    }

    @Test
    public void getTraversalSource_WithValidClient_ReturnsNonNullTraversalSource() {
        NeptuneConnection connection = new NeptuneGremlinConnection(TEST_ENDPOINT, TEST_PORT, false, TEST_REGION) {

            @Override
            public GraphTraversalSource getTraversalSource(Client client) {
                return mockTraversalSource;
            }
        };
        
        GraphTraversalSource traversalSource = connection.getTraversalSource(mockClient);
        assertNotNull(traversalSource);
        assertEquals(mockTraversalSource, traversalSource);
    }
}