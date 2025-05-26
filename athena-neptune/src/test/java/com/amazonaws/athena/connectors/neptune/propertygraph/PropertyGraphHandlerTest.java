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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.neptune.Constants;
import com.amazonaws.athena.connectors.neptune.NeptuneConnection;
import com.amazonaws.athena.connectors.neptune.qpt.NeptuneQueryPassthrough;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import javax.script.ScriptException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PropertyGraphHandlerTest {

    @Mock
    private NeptuneConnection neptuneConnection;
    @Mock
    private Client client;
    @Mock
    private GraphTraversalSource graphTraversalSource;
    @Mock
    private GraphTraversal graphTraversal;
    @Mock
    private BlockSpiller spiller;
    @Mock
    private QueryStatusChecker queryStatusChecker;
    @Mock
    private ReadRecordsRequest recordsRequest;
    @Mock
    private Block block;
    @Mock
    private ResultSet resultSet;
    private PropertyGraphHandler handler;
    private Schema schema;
    private Map<String, String> customMetadata;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        handler = new PropertyGraphHandler(neptuneConnection);
        
        // Setup common mocks
        when(neptuneConnection.getNeptuneClientConnection()).thenReturn(client);
        when(neptuneConnection.getTraversalSource(any())).thenReturn(graphTraversalSource);
        when(queryStatusChecker.isQueryRunning()).thenReturn(true);
        when(graphTraversalSource.V()).thenReturn(graphTraversal);
        when(graphTraversalSource.E()).thenReturn(graphTraversal);
        when(graphTraversal.hasLabel(anyString())).thenReturn(graphTraversal);
        when(graphTraversal.valueMap()).thenReturn(graphTraversal);
        when(graphTraversal.elementMap()).thenReturn(graphTraversal);
        when(graphTraversal.with(any())).thenReturn(graphTraversal);

        // Setup schema
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("id", FieldType.nullable(new ArrowType.Utf8()), null));
        fields.add(new Field("label", FieldType.nullable(new ArrowType.Utf8()), null));
        
        customMetadata = new HashMap<>();
        customMetadata.put(Constants.SCHEMA_COMPONENT_TYPE, "VERTEX"); // Set default type
        schema = new Schema(fields, customMetadata);
        
        when(recordsRequest.getSchema()).thenReturn(schema);
        when(recordsRequest.getTableName()).thenReturn(new TableName("testDb", "testTable"));
        
        // Setup constraints with empty maps and lists
        Map<String, ValueSet> constraintsMap = new HashMap<>();
        Constraints constraints = new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), 0, Collections.emptyMap());
        when(recordsRequest.getConstraints()).thenReturn(constraints);

        // Setup spiller mock to handle writeRows
        doAnswer((Answer<Void>) invocation -> {
            BlockSpiller.RowWriter rowWriter = (BlockSpiller.RowWriter) invocation.getArguments()[0];
            rowWriter.writeRows(block, 1);
            return null;
        }).when(spiller).writeRows(any());
    }

    @Test
    void testVertexQuery() throws Exception {
        // Setup vertex specific mocks
        Map<String, Object> vertexData = new HashMap<>();
        vertexData.put(T.id.toString(), "1");
        vertexData.put(T.label.toString(), "person");
        when(graphTraversal.hasNext()).thenReturn(true, false);
        when(graphTraversal.next()).thenReturn(vertexData);
        
        // Execute test
        handler.executeQuery(recordsRequest, queryStatusChecker, spiller, new HashMap<>());
        
        // Verify
        verify(spiller, times(1)).writeRows(any());
        verify(graphTraversal, atLeastOnce()).hasNext();
        verify(graphTraversal, atLeastOnce()).next();
    }

    @Test
    void testEdgeQuery() throws Exception {
        // Setup edge specific mocks
        customMetadata.put(Constants.SCHEMA_COMPONENT_TYPE, "EDGE");
        Map<String, Object> edgeData = new HashMap<>();
        edgeData.put(T.id.toString(), "1");
        edgeData.put(T.label.toString(), "knows");
        when(graphTraversal.hasNext()).thenReturn(true, false);
        when(graphTraversal.next()).thenReturn(edgeData);
        
        // Execute test
        handler.executeQuery(recordsRequest, queryStatusChecker, spiller, new HashMap<>());
        
        // Verify
        verify(spiller, times(1)).writeRows(any());
        verify(graphTraversal, atLeastOnce()).hasNext();
        verify(graphTraversal, atLeastOnce()).next();
    }

    @Test
    void testViewQuery() throws Exception {
        // Setup view specific mocks
        customMetadata.put(Constants.SCHEMA_COMPONENT_TYPE, "VIEW");
        customMetadata.put(Constants.SCHEMA_QUERY, "g.V().count()");
        
        // Add count field to schema
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("count", FieldType.nullable(new ArrowType.Int(64, true)), null));
        Schema viewSchema = new Schema(fields, customMetadata);
        when(recordsRequest.getSchema()).thenReturn(viewSchema);
        
        Result mockResult = mock(Result.class);
        when(mockResult.getObject()).thenReturn(42L);
        
        @SuppressWarnings("unchecked")
        Iterator<Result> resultIterator = mock(Iterator.class);
        when(resultIterator.hasNext()).thenReturn(true, false);
        when(resultIterator.next()).thenReturn(mockResult);
        
        when(resultSet.iterator()).thenReturn(resultIterator);
        when(client.submit(anyString())).thenReturn(resultSet);
        
        // Execute test
        handler.executeQuery(recordsRequest, queryStatusChecker, spiller, new HashMap<>());
        
        // Verify
        verify(spiller, times(1)).writeRows(any());
        verify(client, times(1)).submit(anyString());
    }

    @Test
    void testViewQueryWithPassthrough() throws Exception {
        // Setup view specific mocks
        customMetadata.put(Constants.SCHEMA_COMPONENT_TYPE, "VIEW");
        
        // Add count field to schema
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("count", FieldType.nullable(new ArrowType.Int(64, true)), null));
        Schema viewSchema = new Schema(fields, customMetadata);
        when(recordsRequest.getSchema()).thenReturn(viewSchema);
        
        // Setup query passthrough
        Map<String, String> qptArgs = new HashMap<>();
        qptArgs.put(NeptuneQueryPassthrough.DATABASE, "testDb");
        qptArgs.put(NeptuneQueryPassthrough.COLLECTION, "person");
        qptArgs.put(NeptuneQueryPassthrough.QUERY, "g.V().count()");
        qptArgs.put(SCHEMA_FUNCTION_NAME, "SYSTEM.QUERY");
        
        Constraints constraints = mock(Constraints.class);
        when(constraints.isQueryPassThrough()).thenReturn(true);
        when(constraints.getQueryPassthroughArguments()).thenReturn(qptArgs);
        when(recordsRequest.getConstraints()).thenReturn(constraints);
        
        Result mockResult = mock(Result.class);
        when(mockResult.getObject()).thenReturn(42L);
        
        @SuppressWarnings("unchecked")
        Iterator<Result> resultIterator = mock(Iterator.class);
        when(resultIterator.hasNext()).thenReturn(true, false);
        when(resultIterator.next()).thenReturn(mockResult);
        
        when(resultSet.iterator()).thenReturn(resultIterator);
        when(client.submit(anyString())).thenReturn(resultSet);
        
        // Execute test
        handler.executeQuery(recordsRequest, queryStatusChecker, spiller, new HashMap<>());
        
        // Verify
        verify(spiller, times(1)).writeRows(any());
        verify(client, times(1)).submit(anyString());
    }

    @Test
    void testQueryPassthrough() throws Exception {
        // Setup query passthrough mocks
        Map<String, String> qptArgs = new HashMap<>();
        qptArgs.put(NeptuneQueryPassthrough.DATABASE, "testDb");
        qptArgs.put(NeptuneQueryPassthrough.COLLECTION, "person");
        qptArgs.put(NeptuneQueryPassthrough.QUERY, "g.V().hasLabel('person')");
        qptArgs.put(SCHEMA_FUNCTION_NAME, "SYSTEM.QUERY");
        
        Constraints constraints = mock(Constraints.class);
        when(constraints.isQueryPassThrough()).thenReturn(true);
        when(constraints.getQueryPassthroughArguments()).thenReturn(qptArgs);
        when(recordsRequest.getConstraints()).thenReturn(constraints);
        
        Map<String, Object> vertexData = new HashMap<>();
        vertexData.put(T.id.toString(), "1");
        vertexData.put(T.label.toString(), "person");
        when(graphTraversal.hasNext()).thenReturn(true, false);
        when(graphTraversal.next()).thenReturn(vertexData);
        
        // Execute test
        handler.executeQuery(recordsRequest, queryStatusChecker, spiller, new HashMap<>());
        
        // Verify
        verify(spiller, times(1)).writeRows(any());
        verify(graphTraversal, atLeastOnce()).hasNext();
        verify(graphTraversal, atLeastOnce()).next();
    }

    @Test
    void testQueryWithGlabel() throws Exception {
        // Setup glabel specific mocks
        customMetadata.put(Constants.SCHEMA_GLABEL, "customLabel");
        when(recordsRequest.getTableName()).thenReturn(new TableName("testDb", "customLabel"));
        
        Map<String, Object> vertexData = new HashMap<>();
        vertexData.put(T.id.toString(), "1");
        vertexData.put(T.label.toString(), "customLabel");
        when(graphTraversal.hasNext()).thenReturn(true, false);
        when(graphTraversal.next()).thenReturn(vertexData);
        
        // Execute test
        handler.executeQuery(recordsRequest, queryStatusChecker, spiller, new HashMap<>());
        
        // Verify
        verify(spiller, times(1)).writeRows(any());
        verify(graphTraversal, atLeastOnce()).hasNext();
        verify(graphTraversal, atLeastOnce()).next();
        verify(graphTraversal).hasLabel("customLabel");
    }

    @Test
    void testQueryTermination() throws Exception {
        // Setup termination test
        when(queryStatusChecker.isQueryRunning()).thenReturn(false);
        
        // Execute test
        handler.executeQuery(recordsRequest, queryStatusChecker, spiller, new HashMap<>());
        
        // Verify
        verify(spiller, never()).writeRows(any());
    }

    @Test
    void testGetResponseFromGremlinQuery() throws Exception {
        String gremlinQuery = "g.V().count()";
        when(graphTraversalSource.V()).thenReturn(graphTraversal);
        when(graphTraversal.count()).thenReturn(graphTraversal);
        when(graphTraversal.next()).thenReturn(42L);
        
        Object result = handler.getResponseFromGremlinQuery(graphTraversalSource, gremlinQuery);
        
        verify(graphTraversalSource).V();
    }

    @Test
    void testGetResponseFromGremlinQueryWithError() {
        String invalidQuery = "invalid.query()";
        
        assertThrows(ScriptException.class, () -> {
            handler.getResponseFromGremlinQuery(graphTraversalSource, invalidQuery);
        });
    }

    @Test
    void testViewQueryWithInvalidSchema() {
        // Setup view specific mocks with invalid schema (no query)
        customMetadata.put(Constants.SCHEMA_COMPONENT_TYPE, "VIEW");
        
        // Add count field to schema
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("count", FieldType.nullable(new ArrowType.Int(64, true)), null));
        Schema viewSchema = new Schema(fields, customMetadata);
        when(recordsRequest.getSchema()).thenReturn(viewSchema);
        
        // Execute test and verify exception
        assertThrows(RuntimeException.class, () -> {
            handler.executeQuery(recordsRequest, queryStatusChecker, spiller, new HashMap<>());
        });
    }

    @Test
    void testQueryPassthroughWithInvalidQuery() {
        // Setup query passthrough mocks with missing query
        Map<String, String> qptArgs = new HashMap<>();
        qptArgs.put(NeptuneQueryPassthrough.DATABASE, "testDb");
        qptArgs.put(NeptuneQueryPassthrough.COLLECTION, "person");
        qptArgs.put(SCHEMA_FUNCTION_NAME, "SYSTEM.QUERY");
        
        Constraints constraints = mock(Constraints.class);
        when(constraints.isQueryPassThrough()).thenReturn(true);
        when(constraints.getQueryPassthroughArguments()).thenReturn(qptArgs);
        when(recordsRequest.getConstraints()).thenReturn(constraints);
        
        // Execute test and verify exception
        assertThrows(RuntimeException.class, () -> {
            handler.executeQuery(recordsRequest, queryStatusChecker, spiller, new HashMap<>());
        });
    }
}
