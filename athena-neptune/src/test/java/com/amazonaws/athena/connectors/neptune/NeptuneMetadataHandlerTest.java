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

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.neptune.propertygraph.PropertyGraphHandler;
import com.amazonaws.athena.connectors.neptune.qpt.NeptuneGremlinQueryPassthrough;
import com.amazonaws.athena.connectors.neptune.qpt.NeptuneSparqlQueryPassthrough;
import com.amazonaws.athena.connectors.neptune.rdf.NeptuneSparqlConnection;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class NeptuneMetadataHandlerTest extends TestBase
{
    @Mock
    private GlueClient glue;
    @Mock
    private NeptuneConnection neptuneConnection;
    private NeptuneMetadataHandler handler;
    private BlockAllocatorImpl allocator;
    private static final Map<String, String> DEFAULT_PARAMS = new HashMap<>();

    @Before
    public void setUp()
    {
        allocator = new BlockAllocatorImpl();
        glue = mock(GlueClient.class);
        handler = new NeptuneMetadataHandler(glue, neptuneConnection, new LocalKeyFactory(),
                mock(SecretsManagerClient.class), mock(AthenaClient.class),
                "spill-bucket", "spill-prefix", DEFAULT_PARAMS);
    }

    @After
    public void after()
    {
        allocator.close();
    }

    @Test
    public void doListSchemaNames()
    {
        ListSchemasRequest req = new ListSchemasRequest(IDENTITY, "queryId", "default");
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);
        assertFalse(res.getSchemas().isEmpty());
    }

    @Test
    public void doListTables()
    {
        List<Table> tables = Arrays.asList(
                Table.builder().name("table1").build(),
                Table.builder().name("table2").build(),
                Table.builder().name("table3").build()
        );
        GetTablesResponse tableResponse = GetTablesResponse.builder().tableList(tables).build();
        when(glue.getTables(any(GetTablesRequest.class))).thenReturn(tableResponse);

        ListTablesRequest req = new ListTablesRequest(IDENTITY, "queryId", "default",
                "default", null, UNLIMITED_PAGE_SIZE_VALUE);
        ListTablesResponse res = handler.doListTables(allocator, req);
        assertFalse(res.getTables().isEmpty());
    }

    @Test
    public void doGetTable() throws Exception
    {
        List<Column> columns = Arrays.asList(
                Column.builder().name("col1").type("int").comment("comment").build(),
                Column.builder().name("col2").type("bigint").comment("comment").build(),
                Column.builder().name("col3").type("string").comment("comment").build(),
                Column.builder().name("col4").type("timestamp").comment("comment").build(),
                Column.builder().name("col5").type("date").comment("comment").build(),
                Column.builder().name("col6").type("timestamptz").comment("comment").build(),
                Column.builder().name("col7").type("timestamptz").comment("comment").build()
        );

        Table table = Table.builder()
                .name("table1")
                .parameters(new HashMap<>())
                .storageDescriptor(StorageDescriptor.builder().columns(columns).build())
                .build();

        when(glue.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class)))
                .thenReturn(software.amazon.awssdk.services.glue.model.GetTableResponse.builder()
                        .table(table).build());

        GetTableRequest req = new GetTableRequest(IDENTITY, "queryId", "default",
                new TableName("schema1", "table1"), Collections.emptyMap());
        GetTableResponse res = handler.doGetTable(allocator, req);
        assertFalse(res.getSchema().getFields().isEmpty());
    }

    private void setupQueryPassthroughTest(String graphType) throws NoSuchFieldException, IllegalAccessException
    {
        Map<String, String> config = new HashMap<>();
        config.put(Constants.CFG_GRAPH_TYPE, graphType);
        Field configOptionsField = handler.getClass().getSuperclass().getSuperclass()
                .getDeclaredField("configOptions");
        configOptionsField.setAccessible(true);
        configOptionsField.set(handler, config);

        List<Column> columns;
        if ("rdf".equals(graphType)) {
            columns = Arrays.asList(
                    Column.builder().name("s").type("string").build(),
                    Column.builder().name("p").type("string").build(),
                    Column.builder().name("o").type("string").build()
            );
        } else {
            columns = Arrays.asList(
                    Column.builder().name("code").type("string").build(),
                    Column.builder().name("city").type("string").build()
            );
        }

        Table table = Table.builder()
                .name("table1")
                .storageDescriptor(StorageDescriptor.builder().columns(columns).build())
                .parameters(new HashMap<>())
                .build();
        when(glue.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class)))
                .thenReturn(software.amazon.awssdk.services.glue.model.GetTableResponse.builder()
                        .table(table).build());
    }

    @Test
    public void doGetQueryPassthroughSchema_WithRdfAndValidSparql_ReturnsValidSchema() throws Exception
    {
        setupQueryPassthroughTest("rdf");

        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("s", "subject1");
        resultMap.put("p", "predicate1");
        resultMap.put("o", "object1");

        NeptuneSparqlConnection sparqlConnection = mock(NeptuneSparqlConnection.class);
        when(sparqlConnection.hasNext()).thenReturn(true, false);
        when(sparqlConnection.next()).thenReturn(resultMap);

        Field connField = NeptuneMetadataHandler.class.getDeclaredField("neptuneConnection");
        connField.setAccessible(true);
        connField.set(handler, sparqlConnection);

        GetTableResponse res = handler.doGetQueryPassthroughSchema(allocator,
                createPassthroughRequest("SELECT ?s ?p ?o WHERE { ?s ?p ?o }"));

        assertNotNull(res);
        assertEquals("table1", res.getTableName().getTableName());
        assertFalse(res.getSchema().getFields().isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void doGetQueryPassthroughSchema_WithInvalidGraphType_ThrowsIllegalArgumentException() throws Exception
    {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(Constants.CFG_GRAPH_TYPE, "INVALID_TYPE");

        Field configOptionsField = handler.getClass().getSuperclass().getSuperclass()
                .getDeclaredField("configOptions");
        configOptionsField.setAccessible(true);
        configOptionsField.set(handler, configOptions);

        Table table = Table.builder()
                .name("table1")
                .storageDescriptor(StorageDescriptor.builder()
                        .columns(Collections.singletonList(
                                Column.builder().name("col1").type("string").build()
                        ))
                        .build())
                .build();

        when(glue.getTable(any(software.amazon.awssdk.services.glue.model.GetTableRequest.class)))
                .thenReturn(software.amazon.awssdk.services.glue.model.GetTableResponse.builder()
                        .table(table)
                        .build());

        GetTableRequest req = createPassthroughRequest("g.V().hasLabel(\"airport\").valueMap()");
        handler.doGetQueryPassthroughSchema(allocator, req);
    }

    @Test
    public void doGetDataSourceCapabilities_WithValidRequest_ReturnsCapabilities()
    {
        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(IDENTITY, "queryId", DEFAULT_CATALOG);
        GetDataSourceCapabilitiesResponse response = handler.doGetDataSourceCapabilities(allocator, request);

        assertNotNull(response);
        assertNotNull(response.getCapabilities());
    }

    @Test(expected = NullPointerException.class)
    public void doGetTable_WithNullGlue_ThrowsNullPointerException() throws Exception
    {
        NeptuneMetadataHandler nullGlueHandler = new NeptuneMetadataHandler(null, neptuneConnection,
                new LocalKeyFactory(), mock(SecretsManagerClient.class),
                mock(AthenaClient.class), "spill-bucket", "spill-prefix", DEFAULT_PARAMS);

        GetTableRequest req = new GetTableRequest(IDENTITY, "queryId", "default",
                new TableName("schema1", "table1"), Collections.emptyMap());
        nullGlueHandler.doGetTable(allocator, req);
    }

    @Test(expected = RuntimeException.class)
    public void doGetQueryPassthroughSchema_WithRdfAndInvalidSparql_ThrowsRuntimeException() throws Exception
    {
        setupQueryPassthroughTest("rdf");

        NeptuneSparqlConnection sparqlConnection = mock(NeptuneSparqlConnection.class);
        when(sparqlConnection.hasNext()).thenReturn(false);
        doThrow(new RuntimeException("Invalid SPARQL query")).when(sparqlConnection).runQuery(anyString());

        Field connField = NeptuneMetadataHandler.class.getDeclaredField("neptuneConnection");
        connField.setAccessible(true);
        connField.set(handler, sparqlConnection);

        handler.doGetQueryPassthroughSchema(allocator, createPassthroughRequest("INVALID SPARQL QUERY"));
    }

    @Test(expected = RuntimeException.class)
    public void doGetQueryPassthroughSchema_WithPropertyGraphAndEmptyResponse_ThrowsRuntimeException() throws Exception
    {
        setupQueryPassthroughTest("propertygraph");

        GraphTraversal graphTraversalMock = mock(GraphTraversal.class);
        when(graphTraversalMock.hasNext()).thenReturn(false);

        Client clientMock = mock(Client.class);
        when(neptuneConnection.getNeptuneClientConnection()).thenReturn(clientMock);
        when(neptuneConnection.getTraversalSource(clientMock))
                .thenReturn(mock(GraphTraversalSource.class));

        try (MockedConstruction<PropertyGraphHandler> mocked =
                     mockConstruction(PropertyGraphHandler.class,
                             (mock, context) -> when(mock.getResponseFromGremlinQuery(any(), anyString()))
                                     .thenReturn(graphTraversalMock))) {

            handler.doGetQueryPassthroughSchema(allocator,
                    createPassthroughRequest("g.V().hasLabel('nonexistent').valueMap().limit(1)"));
        }
    }

    @Test
    public void doGetQueryPassthroughSchema_WithPropertyGraphAndValidGremlin_ReturnsValidSchema() throws Exception
    {
        setupQueryPassthroughTest("propertygraph");

        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("code", "SFO");
        resultMap.put("city", "San Francisco");

        GraphTraversal graphTraversalMock = mock(GraphTraversal.class);
        when(graphTraversalMock.hasNext()).thenReturn(true, false);
        when(graphTraversalMock.next()).thenReturn(resultMap);

        try (MockedConstruction<PropertyGraphHandler> mocked =
                     mockConstruction(PropertyGraphHandler.class,
                             (mock, context) -> when(mock.getResponseFromGremlinQuery(any(), anyString()))
                                     .thenReturn(graphTraversalMock))) {

            GetTableResponse res = handler.doGetQueryPassthroughSchema(allocator,
                    createGremlinPassthroughRequest("g.V().hasLabel(\"airport\").valueMap().limit(1)"));

            assertNotNull(res);
            assertEquals("table1", res.getTableName().getTableName());
            assertFalse(res.getSchema().getFields().isEmpty());
            assertEquals(2, res.getSchema().getFields().size());
        }
    }

    @Test
    public void doGetQueryPassthroughSchema_WithPropertyGraphAndPartialColumns_ReturnsSchemaWithAvailableColumns() throws Exception
    {
        setupQueryPassthroughTest("propertygraph");

        // Create a result map with only one column, while Glue schema has two columns
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("code", "SFO");
        // Note: "city" column is missing from results

        GraphTraversal graphTraversalMock = mock(GraphTraversal.class);
        when(graphTraversalMock.hasNext()).thenReturn(true);
        when(graphTraversalMock.next()).thenReturn(resultMap);

        try (MockedConstruction<PropertyGraphHandler> mocked =
                     mockConstruction(PropertyGraphHandler.class,
                             (mock, context) -> when(mock.getResponseFromGremlinQuery(any(), anyString()))
                                     .thenReturn(graphTraversalMock))) {

            GetTableResponse res = handler.doGetQueryPassthroughSchema(allocator,
                    createGremlinPassthroughRequest("g.V().hasLabel(\"airport\").valueMap(\"code\").limit(1)"));

            assertNotNull(res);
            assertEquals("table1", res.getTableName().getTableName());
            assertFalse(res.getSchema().getFields().isEmpty());
            assertEquals(1, res.getSchema().getFields().size());
            assertEquals("code", res.getSchema().getFields().get(0).getName());
        }
    }

    private GetTableRequest createPassthroughRequest(String query)
    {
        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put(SCHEMA_FUNCTION_NAME, "SYSTEM.QUERY");
        passthroughArgs.put(NeptuneSparqlQueryPassthrough.DATABASE, "schema1");
        passthroughArgs.put(NeptuneSparqlQueryPassthrough.COLLECTION, "table1");
        passthroughArgs.put(NeptuneSparqlQueryPassthrough.QUERY, query);
        return new GetTableRequest(IDENTITY, "queryId", "catalog",
                new TableName("schema1", "table1"), passthroughArgs);
    }

    private GetTableRequest createGremlinPassthroughRequest(String query)
    {
        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put(SCHEMA_FUNCTION_NAME, "SYSTEM.TRAVERSE");
        passthroughArgs.put(NeptuneGremlinQueryPassthrough.DATABASE, "schema1");
        passthroughArgs.put(NeptuneGremlinQueryPassthrough.COLLECTION, "table1");
        passthroughArgs.put(NeptuneGremlinQueryPassthrough.TRAVERSE, query);
        passthroughArgs.put(NeptuneGremlinQueryPassthrough.COMPONENT_TYPE, "valueMap");
        return new GetTableRequest(IDENTITY, "queryId", "catalog",
                new TableName("schema1", "table1"), passthroughArgs);
    }
}
