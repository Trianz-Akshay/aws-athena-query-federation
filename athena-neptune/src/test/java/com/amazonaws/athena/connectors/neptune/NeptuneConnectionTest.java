/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class NeptuneConnectionTest {
    private static final String TEST_ENDPOINT = "localhost";
    private static final String TEST_PORT = "8182";
    private static final String TEST_REGION = "us-east-1";

    @Mock
    private Client mockClient;
    @Mock
    private Cluster mockCluster;
    @Mock
    private GraphTraversalSource mockTraversalSource;
    private Map<String, String> configOptions;

    @Before
    public void setUp() {
        configOptions = new HashMap<>();
        configOptions.put(Constants.CFG_ENDPOINT, TEST_ENDPOINT);
        configOptions.put(Constants.CFG_PORT, TEST_PORT);
        configOptions.put(Constants.CFG_IAM, "false");
        configOptions.put(Constants.CFG_REGION, TEST_REGION);
    }

    @Test
    public void createConnection_WithPropertyGraphType_ReturnsNeptuneGremlinConnection() {
        // Setup
        configOptions.put(Constants.CFG_GRAPH_TYPE, "PROPERTYGRAPH");
        
        // Execute
        NeptuneConnection connection = NeptuneConnection.createConnection(configOptions);
        
        // Verify
        assertNotNull("Connection should not be null", connection);
        assertTrue("Should be instance of NeptuneGremlinConnection", connection instanceof NeptuneGremlinConnection);
        assertEquals("Endpoint should match", TEST_ENDPOINT, connection.getNeptuneEndpoint());
        assertEquals("Port should match", TEST_PORT, connection.getNeptunePort());
        assertEquals("Region should match", TEST_REGION, connection.getRegion());
        assertFalse("IAM should be disabled", connection.isEnabledIAM());
    }

    @Test
    public void createConnection_WithRDFType_ReturnsNeptuneSparqlConnection() {
        // Setup
        configOptions.put(Constants.CFG_GRAPH_TYPE, "RDF");
        
        // Execute
        NeptuneConnection connection = NeptuneConnection.createConnection(configOptions);
        
        // Verify
        assertNotNull("Connection should not be null", connection);
        assertTrue("Should be instance of NeptuneSparqlConnection", connection instanceof NeptuneSparqlConnection);
        assertEquals("Endpoint should match", TEST_ENDPOINT, connection.getNeptuneEndpoint());
        assertEquals("Port should match", TEST_PORT, connection.getNeptunePort());
        assertEquals("Region should match", TEST_REGION, connection.getRegion());
        assertFalse("IAM should be disabled", connection.isEnabledIAM());
    }

    @Test
    public void createConnection_WithNullGraphType_ReturnsNeptuneGremlinConnection() {
        // Setup - don't set graph type to test default behavior
        
        // Execute
        NeptuneConnection connection = NeptuneConnection.createConnection(configOptions);
        
        // Verify
        assertNotNull("Connection should not be null", connection);
        assertTrue("Should be instance of NeptuneGremlinConnection", connection instanceof NeptuneGremlinConnection);
        assertEquals("Endpoint should match", TEST_ENDPOINT, connection.getNeptuneEndpoint());
        assertEquals("Port should match", TEST_PORT, connection.getNeptunePort());
        assertEquals("Region should match", TEST_REGION, connection.getRegion());
        assertFalse("IAM should be disabled", connection.isEnabledIAM());
    }

    @Test(expected = IllegalArgumentException.class)
    public void createConnection_WithInvalidGraphType_ThrowsIllegalArgumentException() {
        // Setup
        configOptions.put(Constants.CFG_GRAPH_TYPE, "INVALID_TYPE");
        
        // Execute - should throw IllegalArgumentException
        NeptuneConnection.createConnection(configOptions);
    }

    @Test
    public void createConnection_WithIAMEnabled_ReturnsConnectionWithIAMEnabled() {
        // Setup
        configOptions.put(Constants.CFG_GRAPH_TYPE, "PROPERTYGRAPH");
        configOptions.put(Constants.CFG_IAM, "true");
        
        // Execute
        NeptuneConnection connection = NeptuneConnection.createConnection(configOptions);
        
        // Verify
        assertNotNull("Connection should not be null", connection);
        assertTrue("Should be instance of NeptuneGremlinConnection", connection instanceof NeptuneGremlinConnection);
        assertTrue("IAM should be enabled", connection.isEnabledIAM());
    }

    @Test
    public void getNeptuneClientConnection_WithValidConnection_ReturnsNonNullClient() {
        NeptuneConnection connection = new NeptuneGremlinConnection(TEST_ENDPOINT, TEST_PORT, false, TEST_REGION) {
            protected Cluster createCluster() {
                return mockCluster;
            }

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
            protected Cluster createCluster() {
                return mockCluster;
            }

            @Override
            public GraphTraversalSource getTraversalSource(Client client) {
                return mockTraversalSource;
            }
        };
        
        GraphTraversalSource traversalSource = connection.getTraversalSource(mockClient);
        assertNotNull(traversalSource);
        assertEquals(mockTraversalSource, traversalSource);
    }

    @Test
    public void closeCluster_WhenCalled_InvokesClusterClose() {
        NeptuneConnection connection = new NeptuneGremlinConnection(TEST_ENDPOINT, TEST_PORT, false, TEST_REGION) {
            protected Cluster createCluster() {
                return mockCluster;
            }

            @Override
            public void closeCluster() {
                mockCluster.close();
            }
        };
        
        connection.closeCluster();
        verify(mockCluster).close();
    }
} 