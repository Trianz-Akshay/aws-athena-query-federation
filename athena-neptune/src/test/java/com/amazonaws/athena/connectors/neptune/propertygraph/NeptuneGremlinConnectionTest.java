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
package com.amazonaws.athena.connectors.neptune.propertygraph;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.neptune.auth.NeptuneNettyHttpSigV4Signer;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class NeptuneGremlinConnectionTest {

    private static final String TEST_ENDPOINT = "test-endpoint";
    private static final String TEST_PORT = "8182";
    private static final String TEST_REGION = "us-east-1";
    
    private NeptuneGremlinConnection connection;
    private Cluster mockCluster;
    private Client mockClient;
    private GraphTraversalSource mockTraversalSource;

    @BeforeEach
    void setUp() {
        mockCluster = mock(Cluster.class);
        mockClient = mock(Client.class);
        mockTraversalSource = mock(GraphTraversalSource.class);
    }

    @AfterEach
    void tearDown() {
        if (connection != null) {
            connection.closeCluster();
        }
    }

    @Test
    void testConstructorWithoutIAM() {
        try (MockedStatic<Cluster> mockedCluster = mockStatic(Cluster.class)) {
            // Mock Cluster.build()
            Cluster.Builder mockBuilder = mock(Cluster.Builder.class);
            mockedCluster.when(Cluster::build).thenReturn(mockBuilder);
            
            // Mock builder method chaining
            when(mockBuilder.addContactPoint(TEST_ENDPOINT)).thenReturn(mockBuilder);
            when(mockBuilder.port(8182)).thenReturn(mockBuilder);
            when(mockBuilder.enableSsl(true)).thenReturn(mockBuilder);
            when(mockBuilder.create()).thenReturn(mockCluster);
            
            // Create connection
            connection = new NeptuneGremlinConnection(TEST_ENDPOINT, TEST_PORT, false, TEST_REGION);
            
            // Verify builder calls - expect 2 calls due to parent class
            verify(mockBuilder, times(2)).addContactPoint(TEST_ENDPOINT);
            verify(mockBuilder, times(2)).port(8182);
            verify(mockBuilder, times(2)).enableSsl(true);
            verify(mockBuilder, times(2)).create();
        }
    }

    @Test
    void testConstructorWithIAM() {
        try (MockedStatic<Cluster> mockedCluster = mockStatic(Cluster.class)) {
            // Mock Cluster.build()
            Cluster.Builder mockBuilder = mock(Cluster.Builder.class);
            mockedCluster.when(Cluster::build).thenReturn(mockBuilder);
            
            // Mock builder method chaining
            when(mockBuilder.addContactPoint(TEST_ENDPOINT)).thenReturn(mockBuilder);
            when(mockBuilder.port(8182)).thenReturn(mockBuilder);
            when(mockBuilder.enableSsl(true)).thenReturn(mockBuilder);
            when(mockBuilder.handshakeInterceptor(any())).thenReturn(mockBuilder);
            when(mockBuilder.create()).thenReturn(mockCluster);
            
            // Create connection
            connection = new NeptuneGremlinConnection(TEST_ENDPOINT, TEST_PORT, true, TEST_REGION);
            
            // Verify builder calls - expect 2 calls due to parent class
            verify(mockBuilder, times(2)).addContactPoint(TEST_ENDPOINT);
            verify(mockBuilder, times(2)).port(8182);
            verify(mockBuilder, times(2)).enableSsl(true);
            verify(mockBuilder, times(2)).handshakeInterceptor(any());
            verify(mockBuilder, times(2)).create();
        }
    }

    @Test
    void testGetNeptuneClientConnection() {
        try (MockedStatic<Cluster> mockedCluster = mockStatic(Cluster.class)) {
            // Mock Cluster.build()
            Cluster.Builder mockBuilder = mock(Cluster.Builder.class);
            mockedCluster.when(Cluster::build).thenReturn(mockBuilder);
            
            // Mock builder method chaining
            when(mockBuilder.addContactPoint(TEST_ENDPOINT)).thenReturn(mockBuilder);
            when(mockBuilder.port(8182)).thenReturn(mockBuilder);
            when(mockBuilder.enableSsl(true)).thenReturn(mockBuilder);
            when(mockBuilder.create()).thenReturn(mockCluster);
            
            // Mock cluster.connect()
            when(mockCluster.connect()).thenReturn(mockClient);
            
            // Create connection and get client
            connection = new NeptuneGremlinConnection(TEST_ENDPOINT, TEST_PORT, false, TEST_REGION);
            Client client = connection.getNeptuneClientConnection();
            
            // Verify
            assertNotNull(client);
            verify(mockCluster).connect();
        }
    }

    @Test
    void testGetTraversalSource() {
        try (MockedStatic<Cluster> mockedCluster = mockStatic(Cluster.class);
             MockedStatic<DriverRemoteConnection> mockedConnection = mockStatic(DriverRemoteConnection.class)) {
            
            // Mock Cluster.build()
            Cluster.Builder mockBuilder = mock(Cluster.Builder.class);
            mockedCluster.when(Cluster::build).thenReturn(mockBuilder);
            
            // Mock builder method chaining
            when(mockBuilder.addContactPoint(TEST_ENDPOINT)).thenReturn(mockBuilder);
            when(mockBuilder.port(8182)).thenReturn(mockBuilder);
            when(mockBuilder.enableSsl(true)).thenReturn(mockBuilder);
            when(mockBuilder.create()).thenReturn(mockCluster);
            
            // Mock DriverRemoteConnection
            DriverRemoteConnection mockRemoteConnection = mock(DriverRemoteConnection.class);
            mockedConnection.when(() -> DriverRemoteConnection.using(mockClient))
                          .thenReturn(mockRemoteConnection);
            
            // Create connection and get traversal source
            connection = new NeptuneGremlinConnection(TEST_ENDPOINT, TEST_PORT, false, TEST_REGION);
            GraphTraversalSource traversalSource = connection.getTraversalSource(mockClient);
            
            // Verify
            assertNotNull(traversalSource);
            mockedConnection.verify(() -> DriverRemoteConnection.using(mockClient));
        }
    }

    @Test
    void testCloseCluster() {
        try (MockedStatic<Cluster> mockedCluster = mockStatic(Cluster.class)) {
            // Mock Cluster.build()
            Cluster.Builder mockBuilder = mock(Cluster.Builder.class);
            mockedCluster.when(Cluster::build).thenReturn(mockBuilder);
            
            // Mock builder method chaining
            when(mockBuilder.addContactPoint(TEST_ENDPOINT)).thenReturn(mockBuilder);
            when(mockBuilder.port(8182)).thenReturn(mockBuilder);
            when(mockBuilder.enableSsl(true)).thenReturn(mockBuilder);
            when(mockBuilder.create()).thenReturn(mockCluster);
            
            // Create connection and close cluster
            connection = new NeptuneGremlinConnection(TEST_ENDPOINT, TEST_PORT, false, TEST_REGION);
            connection.closeCluster();
            
            // Verify
            verify(mockCluster).close();
        }
    }
} 