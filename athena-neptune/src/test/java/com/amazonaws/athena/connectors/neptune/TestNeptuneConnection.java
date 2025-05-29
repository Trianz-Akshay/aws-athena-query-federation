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

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class TestNeptuneConnection extends NeptuneConnection {
    private final Client mockClient;
    private final GraphTraversalSource mockTraversalSource;

    public TestNeptuneConnection(Client mockClient, GraphTraversalSource mockTraversalSource) {
        super("localhost", "8182", false, "test-iam-role");
        this.mockClient = mockClient;
        this.mockTraversalSource = mockTraversalSource;
    }

    @Override
    public Client getNeptuneClientConnection() {
        return mockClient;
    }

    @Override
    public GraphTraversalSource getTraversalSource(Client client) {
        return mockTraversalSource;
    }

    protected Cluster createCluster() {
        return null;
    }
} 