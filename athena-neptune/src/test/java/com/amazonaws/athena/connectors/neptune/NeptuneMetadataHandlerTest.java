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
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.neptune.propertygraph.PropertyGraphHandler;
import com.amazonaws.athena.connectors.neptune.qpt.NeptuneQueryPassthrough;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.lang.reflect.Field;
import java.util.ArrayList;
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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class NeptuneMetadataHandlerTest extends TestBase {
    private static final Logger logger = LoggerFactory.getLogger(NeptuneMetadataHandlerTest.class);

    @Mock
    private GlueClient glue;

    private NeptuneMetadataHandler handler = null;

    private boolean enableTests = System.getenv("publishing") != null
            && System.getenv("publishing").equalsIgnoreCase("true");

    private BlockAllocatorImpl allocator;

    @Mock
    private NeptuneConnection neptuneConnection;

    @Before
    public void setUp() throws Exception {
        logger.info("setUpBefore - enter");
        allocator = new BlockAllocatorImpl();
        handler = new NeptuneMetadataHandler(glue,neptuneConnection,
                new LocalKeyFactory(), mock(SecretsManagerClient.class), mock(AthenaClient.class), "spill-bucket",
                "spill-prefix", com.google.common.collect.ImmutableMap.of());
        logger.info("setUpBefore - exit");
    }

    @After
    public void after() {
        allocator.close();
    }

    @Test
    public void doListSchemaNames() {
        logger.info("doListSchemas - enter");
        ListSchemasRequest req = new ListSchemasRequest(IDENTITY, "queryId", "default");

        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);
        logger.info("doListSchemas - {}", res.getSchemas());
        assertFalse(res.getSchemas().isEmpty());
        logger.info("doListSchemas - exit");
    }

    @Test
    public void doListTables() {

        logger.info("doListTables - enter");

        List<Table> tables = new ArrayList<Table>();
        Table table1 = Table.builder().name("table1").build();
        Table table2 = Table.builder().name("table2").build();
        Table table3 = Table.builder().name("table3").build();

        tables.add(table1);
        tables.add(table2);
        tables.add(table3);

        GetTablesResponse tableResponse = GetTablesResponse.builder().tableList(tables).build();

        ListTablesRequest req = new ListTablesRequest(IDENTITY, "queryId", "default",
                "default", null, UNLIMITED_PAGE_SIZE_VALUE);
        when(glue.getTables(nullable(GetTablesRequest.class))).thenReturn(tableResponse);

        ListTablesResponse res = handler.doListTables(allocator, req);

        logger.info("doListTables - {}", res.getTables());
        assertFalse(res.getTables().isEmpty());
        logger.info("doListTables - exit");
    }

    @Test
    public void doGetTable() throws Exception {

        logger.info("doGetTable - enter");

        Map<String, String> expectedParams = new HashMap<>();

        List<Column> columns = new ArrayList<>();
        columns.add(Column.builder().name("col1").type("int").comment("comment").build());
        columns.add(Column.builder().name("col2").type("bigint").comment("comment").build());
        columns.add(Column.builder().name("col3").type("string").comment("comment").build());
        columns.add(Column.builder().name("col4").type("timestamp").comment("comm.build()ent").build());
        columns.add(Column.builder().name("col5").type("date").comment("comment").build());
        columns.add(Column.builder().name("col6").type("timestamptz").comment("comment").build());
        columns.add(Column.builder().name("col7").type("timestamptz").comment("comment").build());

        StorageDescriptor storageDescriptor = StorageDescriptor.builder().columns(columns).build();
        Table table = Table.builder()
                .name("table1")
                .parameters(expectedParams)
                .storageDescriptor(storageDescriptor)
                .build();

        expectedParams.put("sourceTable", table.name());
        expectedParams.put("columnMapping", "col2=Col2,col3=Col3, col4=Col4");
        expectedParams.put("datetimeFormatMapping", "col2=someformat2, col1=someformat1 ");

        GetTableRequest req = new GetTableRequest(IDENTITY, "queryId", "default", new TableName("schema1", "table1"), Collections.emptyMap());

        software.amazon.awssdk.services.glue.model.GetTableResponse getTableResponse = software.amazon.awssdk.services.glue.model.GetTableResponse.builder().table(table).build();

        when(glue.getTable(nullable(software.amazon.awssdk.services.glue.model.GetTableRequest.class))).thenReturn(getTableResponse);

        GetTableResponse res = handler.doGetTable(allocator, req);

        assertTrue(res.getSchema().getFields().size() > 0);

        logger.info("doGetTable - {}", res);
        logger.info("doGetTable - exit");
    }

    @Test
    public void testDoGetQueryPassthroughSchema_propertyGraph_validValueMap() throws Exception {
        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put(SCHEMA_FUNCTION_NAME, "SYSTEM.QUERY");
        passthroughArgs.put(NeptuneQueryPassthrough.DATABASE, "schema1");
        passthroughArgs.put(NeptuneQueryPassthrough.COLLECTION, "table1");
        passthroughArgs.put(NeptuneQueryPassthrough.QUERY, "g.V().hasLabel(\"airport\").valueMap()");

        GetTableRequest req = new GetTableRequest(IDENTITY, "queryId", "catalog", new TableName("schema1", "table1"), passthroughArgs);

        Map<String, String> config = new HashMap<>();
        config.put(Constants.CFG_GRAPH_TYPE, "propertygraph");

        // Inject config into handler
        Field configOptionsField = handler.getClass().getSuperclass().getSuperclass().getDeclaredField("configOptions");
        configOptionsField.setAccessible(true);
        configOptionsField.set(handler, config);

        // Mock glue response for doGetTable
        List<Column> columns = Arrays.asList(
                Column.builder().name("code").type("string").build(),
                Column.builder().name("city").type("string").build()
        );
        StorageDescriptor sd = StorageDescriptor.builder().columns(columns).build();
        Table table = Table.builder().name("table1").storageDescriptor(sd).parameters(new HashMap<>()).build();
        software.amazon.awssdk.services.glue.model.GetTableResponse glueResponse =
                software.amazon.awssdk.services.glue.model.GetTableResponse.builder().table(table).build();
        when(glue.getTable((software.amazon.awssdk.services.glue.model.GetTableRequest) any())).thenReturn(glueResponse);

        // Mock traversal
        GraphTraversal graphTraversalMock = mock(GraphTraversal.class);
        when(graphTraversalMock.hasNext()).thenReturn(true);
        Map<String, Object> gremlinResult = new HashMap<>();
        gremlinResult.put("code", "SEA");
        gremlinResult.put("city", "Seattle");
        when(graphTraversalMock.next()).thenReturn(gremlinResult);

        // Mock Neptune connection + property graph handler
        Client clientMock = mock(Client.class);
        when(neptuneConnection.getNeptuneClientConnection()).thenReturn(clientMock);
        when(neptuneConnection.getTraversalSource(clientMock)).thenReturn(mock(GraphTraversalSource.class));

        MockedConstruction<PropertyGraphHandler> mockedDefaultPropertyGraphHandler = mockConstruction(PropertyGraphHandler.class,
                (mock, context) -> {
                    when(mock.getResponseFromGremlinQuery(any(), anyString())).thenReturn(graphTraversalMock);
                });

        GetTableResponse res = handler.doGetQueryPassthroughSchema(allocator, req);

        assertNotNull(res);
        assertEquals("table1", res.getTableName().getTableName());
        assertFalse(res.getSchema().getFields().isEmpty());
        mockedDefaultPropertyGraphHandler.close();
    }

    @Test
    public void testDoGetQueryPassthroughSchema_rdf_validSparql() throws Exception {
        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put(SCHEMA_FUNCTION_NAME, "SYSTEM.QUERY");
        passthroughArgs.put(NeptuneQueryPassthrough.DATABASE, "schema1");
        passthroughArgs.put(NeptuneQueryPassthrough.COLLECTION, "table1");
        passthroughArgs.put(NeptuneQueryPassthrough.QUERY, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }");

        GetTableRequest req = new GetTableRequest(IDENTITY, "queryId", "catalog", new TableName("schema1", "table1"), passthroughArgs);

        Map<String, String> config = new HashMap<>();
        config.put(Constants.CFG_GRAPH_TYPE, "rdf");

        Field configOptionsField = handler.getClass().getSuperclass().getSuperclass().getDeclaredField("configOptions");
        configOptionsField.setAccessible(true);
        configOptionsField.set(handler, config);

        List<Column> columns = Arrays.asList(
                Column.builder().name("s").type("string").build(),
                Column.builder().name("p").type("string").build(),
                Column.builder().name("o").type("string").build()
        );
        StorageDescriptor sd = StorageDescriptor.builder().columns(columns).build();
        Table table = Table.builder().name("table1").storageDescriptor(sd).parameters(new HashMap<>()).build();
        software.amazon.awssdk.services.glue.model.GetTableResponse glueResponse =
                software.amazon.awssdk.services.glue.model.GetTableResponse.builder().table(table).build();
        when(glue.getTable((software.amazon.awssdk.services.glue.model.GetTableRequest) any())).thenReturn(glueResponse);

        NeptuneSparqlConnection sparqlConnection = mock(NeptuneSparqlConnection.class);
        when(sparqlConnection.hasNext()).thenReturn(true);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("s", "subject1");
        resultMap.put("p", "predicate1");
        resultMap.put("o", "object1");
        when(sparqlConnection.next(anyBoolean())).thenReturn(resultMap);

        // Replace neptuneConnection with mocked sparql connection
        Field connField = NeptuneMetadataHandler.class.getDeclaredField("neptuneConnection");
        connField.setAccessible(true);
        connField.set(handler, sparqlConnection);

        GetTableResponse res = handler.doGetQueryPassthroughSchema(allocator, req);

        assertNotNull(res);
        assertEquals("table1", res.getTableName().getTableName());
        assertFalse(res.getSchema().getFields().isEmpty());
    }

    @Test
    public void testDoGetQueryPassthroughSchema_propertyGraph_partialColumns_shouldTriggerElseBlock() throws Exception {
        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put(SCHEMA_FUNCTION_NAME, "SYSTEM.QUERY");
        passthroughArgs.put(NeptuneQueryPassthrough.DATABASE, "schema1");
        passthroughArgs.put(NeptuneQueryPassthrough.COLLECTION, "table1");
        passthroughArgs.put(NeptuneQueryPassthrough.QUERY, "g.V().hasLabel(\"airport\").valueMap(\"code\")");

        GetTableRequest req = new GetTableRequest(IDENTITY, "queryId", "catalog", new TableName("schema1", "table1"), passthroughArgs);

        // Configure handler with propertygraph config
        Map<String, String> config = new HashMap<>();
        config.put(Constants.CFG_GRAPH_TYPE, "propertygraph");
        Field configOptionsField = handler.getClass().getSuperclass().getSuperclass().getDeclaredField("configOptions");
        configOptionsField.setAccessible(true);
        configOptionsField.set(handler, config);

        // Glue schema with 2 columns
        List<Column> glueColumns = Arrays.asList(
                Column.builder().name("code").type("string").build(),
                Column.builder().name("city").type("string").build()
        );
        StorageDescriptor sd = StorageDescriptor.builder().columns(glueColumns).build();
        Table table = Table.builder().name("table1").storageDescriptor(sd).parameters(new HashMap<>()).build();
        software.amazon.awssdk.services.glue.model.GetTableResponse glueResponse =
                software.amazon.awssdk.services.glue.model.GetTableResponse.builder().table(table).build();
        when(glue.getTable((software.amazon.awssdk.services.glue.model.GetTableRequest) any())).thenReturn(glueResponse);

        // Simulate Gremlin response with fewer fields than Glue schema
        GraphTraversal graphTraversalMock = mock(GraphTraversal.class);
        when(graphTraversalMock.hasNext()).thenReturn(true);
        Map<String, Object> gremlinResult = new HashMap<>();
        gremlinResult.put("code", "SEA"); // only one field, will trigger the 'else' block
        when(graphTraversalMock.next()).thenReturn(gremlinResult);

        Client clientMock = mock(Client.class);
        when(neptuneConnection.getNeptuneClientConnection()).thenReturn(clientMock);
        when(neptuneConnection.getTraversalSource(clientMock)).thenReturn(mock(GraphTraversalSource.class));

        try (MockedConstruction<PropertyGraphHandler> mocked = mockConstruction(PropertyGraphHandler.class,
                (mock, context) -> {
                    when(mock.getResponseFromGremlinQuery(any(), anyString())).thenReturn(graphTraversalMock);
                })) {

            GetTableResponse res = handler.doGetQueryPassthroughSchema(allocator, req);

            assertNotNull(res);
            assertEquals("table1", res.getTableName().getTableName());
            assertFalse(res.getSchema().getFields().isEmpty());
            assertEquals(1, res.getSchema().getFields().size()); // Only one field "code" should be in schema
            assertEquals("code", res.getSchema().getFields().get(0).getName());
        }
    }
}
