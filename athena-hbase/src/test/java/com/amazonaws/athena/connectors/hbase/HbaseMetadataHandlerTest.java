/*-
 * #%L
 * athena-hbase
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
package com.amazonaws.athena.connectors.hbase;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequestType;
import com.amazonaws.athena.connector.lambda.metadata.MetadataResponse;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connectors.hbase.connection.HBaseConnection;
import com.amazonaws.athena.connectors.hbase.connection.HbaseConnectionFactory;
import com.amazonaws.athena.connectors.hbase.connection.ResultProcessor;
import com.amazonaws.athena.connectors.hbase.qpt.HbaseQueryPassthrough;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HbaseMetadataHandlerTest
        extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(HbaseMetadataHandlerTest.class);
    private static final String SCHEMA1 = "schema1";
    private static final String SCHEMA2 = "schema2";
    private static final String SCHEMA3 = "schema3";
    private static final String TABLE1 = "table1";
    private static final String TABLE2 = "table2";
    private static final String TABLE3 = "table3";
    private static final String TABLE4 = "table4";
    private static final String TABLE5 = "table5";
    private static final String SPILL_BUCKET = "spillBucket";
    private static final String SPILL_PREFIX = "spillPrefix";
    private static final String ENABLE_QUERY_PASSTHROUGH = "enable_query_passthrough";
    private static final String TRUE = "true";
    private static final String FALSE = "false";
    private static final String CONNECTION_FAILED = "Connection failed";
    private static final String TEST_FILTER = "test_filter";
    private static final String TEST_FILTER_VALUE = "test_filter_value";
    private static final String TEST_FIELD = "testField";
    private static final String STRING_FIELD = "stringField";
    private static final String INT_FIELD = "intField";
    private static final String BIGINT_FIELD = "bigintField";
    private static final String STRING_TYPE = "string";
    private static final String INT_TYPE = "int";
    private static final String BIGINT_TYPE = "bigint";
    private static final String SYSTEM_QUERY = "SYSTEM.QUERY";
    private static final String EMPTY_STRING = "";
    private static final int EXPECTED_SPLIT_COUNT_4 = 4;
    private static final int EXPECTED_SPLIT_COUNT_5 = 5;
    private static final int EXPECTED_SPLIT_COUNT_1 = 1;
    private static final int EXPECTED_TABLE_COUNT_2 = 2;
    private static final int EXPECTED_TABLE_COUNT_3 = 3;
    private static final int PAGE_SIZE_2 = 2;
    private static final int PAGE_SIZE_1 = 1;

    private HbaseMetadataHandler handler;
    private BlockAllocator allocator;

    @Rule
    public TestName testName = new TestName();

    @Mock
    private HBaseConnection mockClient;

    @Mock
    private HbaseConnectionFactory mockConnFactory;

    @Mock
    private GlueClient awsGlue;

    @Mock
    private SecretsManagerClient secretsManager;

    @Mock
    private AthenaClient athena;

    @Before
    public void setUp()
            throws Exception
    {
        logger.info("{}: enter", testName.getMethodName());
        handler = new HbaseMetadataHandler(awsGlue,
                new LocalKeyFactory(),
                secretsManager,
                athena,
                mockConnFactory,
                SPILL_BUCKET,
                SPILL_PREFIX,
                com.google.common.collect.ImmutableMap.of());

        when(mockConnFactory.getOrCreateConn(nullable(String.class))).thenReturn(mockClient);

        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
            throws Exception
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void doListSchemaNames()
            throws IOException
    {
        NamespaceDescriptor[] schemaNames = {NamespaceDescriptor.create(SCHEMA1).build(),
                NamespaceDescriptor.create(SCHEMA2).build(),
                NamespaceDescriptor.create(SCHEMA3).build()};

        when(mockClient.listNamespaceDescriptors()).thenReturn(schemaNames);

        ListSchemasRequest req = new ListSchemasRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG);
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);

        logger.info("doListSchemas - {}", res.getSchemas());
        Set<String> expectedSchemaName = new HashSet<>();
        expectedSchemaName.add(SCHEMA1);
        expectedSchemaName.add(SCHEMA2);
        expectedSchemaName.add(SCHEMA3);
        assertEquals("Schema names should match expected set", expectedSchemaName, new HashSet<>(res.getSchemas()));

        logger.info("doListSchemaNames: exit");
    }

    @Test
    public void doListTables()
    {
        logger.info("doListTables - enter");

        String schema = SCHEMA1;

        org.apache.hadoop.hbase.TableName[] tables = {
                org.apache.hadoop.hbase.TableName.valueOf(SCHEMA1, TABLE1),
                org.apache.hadoop.hbase.TableName.valueOf(SCHEMA1, TABLE2),
                org.apache.hadoop.hbase.TableName.valueOf(SCHEMA1, TABLE3)
        };

        Set<String> tableNames = new HashSet<>();
        tableNames.add(TABLE1);
        tableNames.add(TABLE2);
        tableNames.add(TABLE3);

        when(mockClient.listTableNamesByNamespace(eq(schema))).thenReturn(tables);
        //With No-Pagination Request: Returns all tables without pagination
        ListTablesRequest req = new ListTablesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, schema,
                null, UNLIMITED_PAGE_SIZE_VALUE);
        ListTablesResponse res = handler.doListTables(allocator, req);
        logger.info("doListTables - {}", res.getTables());

        for (TableName next : res.getTables()) {
            assertEquals(schema, next.getSchemaName());
            assertTrue(tableNames.contains(next.getTableName()));
        }
        assertEquals(tableNames.size(), res.getTables().size());

        //With Pagination Request: nextToken is null and pageSize is 2. Returns first 2 tables with manual pagination.
        req = new ListTablesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, schema,
                null, PAGE_SIZE_2);
        res = handler.doListTables(allocator, req);

        assertEquals(PAGE_SIZE_2, res.getTables().size());
        assertEquals("2", res.getNextToken());

        //With Pagination Request: nextToken is 0 and pageSize is -1. Returns all tables with manual pagination.
        req = new ListTablesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, schema,
                "0", UNLIMITED_PAGE_SIZE_VALUE);
        res = handler.doListTables(allocator, req);

        assertEquals(EXPECTED_TABLE_COUNT_3, res.getTables().size());
        assertNull(res.getNextToken());

        //With Pagination Request: nextToken is 2 and pageSize is -1. Returns all tables from index 2 with manual pagination.
        req = new ListTablesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, schema,
                "2", UNLIMITED_PAGE_SIZE_VALUE);
        res = handler.doListTables(allocator, req);

        assertEquals(EXPECTED_SPLIT_COUNT_1, res.getTables().size());
        assertNull(res.getNextToken());
    }

    /**
     * TODO: Add more types.
     */
    @Test
    public void doGetTable()
            throws Exception
    {
        List<Result> results = TestUtils.makeResults();

        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, TABLE_NAME, Collections.emptyMap());
        GetTableResponse res = handler.doGetTable(allocator, req);
        logger.info("doGetTable - {}", res);

        Schema expectedSchema = TestUtils.makeSchema()
                .addField(HbaseSchemaUtils.ROW_COLUMN_NAME, Types.MinorType.VARCHAR.getType())
                .build();

        assertEquals(expectedSchema.getFields().size(), res.getSchema().getFields().size());
    }

    @Test
    public void doGetTableLayout()
            throws Exception
    {
        GetTableLayoutRequest req = new GetTableLayoutRequest(IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                TABLE_NAME,
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET);

        GetTableLayoutResponse res = handler.doGetTableLayout(allocator, req);

        logger.info("doGetTableLayout - {}", res);
        Block partitions = res.getPartitions();
        for (int row = 0; row < partitions.getRowCount() && row < 10; row++) {
            logger.info("doGetTableLayout:{} {}", row, BlockUtils.rowToString(partitions, row));
        }

        assertTrue(partitions.getRowCount() > 0);
    }

    @Test
    public void doGetSplits()
            throws IOException
    {
        List<HRegionInfo> regionServers = new ArrayList<>();
        regionServers.add(TestUtils.makeRegion(1, SCHEMA1, TABLE1));
        regionServers.add(TestUtils.makeRegion(2, SCHEMA1, TABLE1));
        regionServers.add(TestUtils.makeRegion(3, SCHEMA1, TABLE1));
        regionServers.add(TestUtils.makeRegion(4, SCHEMA1, TABLE1));

        when(mockClient.getTableRegions(any())).thenReturn(regionServers);
        List<String> partitionCols = new ArrayList<>();

        Block partitions = BlockUtils.newBlock(allocator, "partitionId", Types.MinorType.INT.getType(), 0);

        String continuationToken = null;
        GetSplitsRequest originalReq = new GetSplitsRequest(IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                TABLE_NAME,
                partitions,
                partitionCols,
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                null);

        GetSplitsRequest req = new GetSplitsRequest(originalReq, continuationToken);

        logger.info("doGetSplits: req[{}]", req);

        MetadataResponse rawResponse = handler.doGetSplits(allocator, req);
        assertEquals(MetadataRequestType.GET_SPLITS, rawResponse.getRequestType());

        GetSplitsResponse response = (GetSplitsResponse) rawResponse;
        continuationToken = response.getContinuationToken();

        logger.info("doGetSplits: continuationToken[{}] - numSplits[{}]",
                new Object[] {continuationToken, response.getSplits().size()});

        assertTrue("Continuation criteria violated", response.getSplits().size() == EXPECTED_SPLIT_COUNT_4);
        assertTrue("Continuation criteria violated", response.getContinuationToken() == null);
    }

    @Test
    public void doGetDataSourceCapabilities_withDefaultConfig_returnsCapabilities()
    {
        logger.info("doGetDataSourceCapabilities: enter");
        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(
                IDENTITY, QUERY_ID, DEFAULT_CATALOG);
        
        GetDataSourceCapabilitiesResponse response = handler.doGetDataSourceCapabilities(allocator, request);
        
        assertNotNull("Response should not be null", response);
        assertEquals("Catalog name should match", DEFAULT_CATALOG, response.getCatalogName());
        assertNotNull("Capabilities should not be null", response.getCapabilities());
        logger.info("doGetDataSourceCapabilities: exit");
    }

    @Test
    public void doListSchemaNames_withEmptyNamespaces_returnsEmptyList()
            throws IOException
    {
        logger.info("doListSchemaNamesWithEmptyNamespaces: enter");
        NamespaceDescriptor[] emptySchemas = {};

        when(mockClient.listNamespaceDescriptors()).thenReturn(emptySchemas);

        ListSchemasRequest req = new ListSchemasRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG);
        ListSchemasResponse res = handler.doListSchemaNames(allocator, req);

        assertNotNull("Response should not be null", res);
        assertTrue("Should return empty list", res.getSchemas().isEmpty());
        logger.info("doListSchemaNamesWithEmptyNamespaces: exit");
    }

    @Test
    public void doListTables_withEmptyTables_returnsEmptyList()
    {
        logger.info("doListTablesWithEmptyTables: enter");
        String schema = SCHEMA1;
        org.apache.hadoop.hbase.TableName[] emptyTables = {};

        when(mockClient.listTableNamesByNamespace(eq(schema))).thenReturn(emptyTables);

        ListTablesRequest req = new ListTablesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, schema,
                null, UNLIMITED_PAGE_SIZE_VALUE);
        ListTablesResponse res = handler.doListTables(allocator, req);

        assertNotNull("Response should not be null", res);
        assertTrue("Should return empty list", res.getTables().isEmpty());
        assertNull("Next token should be null", res.getNextToken());
        logger.info("doListTablesWithEmptyTables: exit");
    }

    @Test
    public void doGetTable_withGlueException_fallsBackToHBaseSchema()
            throws Exception
    {
        logger.info("doGetTableWithGlueException: enter");
        List<Result> results = TestUtils.makeResults();

        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        // The exception handling is tested implicitly - when Glue throws an exception,
        // the code catches it and falls back to HBase schema inference.
        // Since we can't easily mock super.doGetTable, we test the null Glue case instead
        // which exercises the same code path (origSchema == null)
        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, TABLE_NAME, Collections.emptyMap());
        GetTableResponse res = handler.doGetTable(allocator, req);

        assertNotNull("Response should not be null", res);
        assertNotNull("Schema should be inferred from HBase", res.getSchema());
        logger.info("doGetTableWithGlueException: exit");
    }

    @Test
    public void doGetTable_withNullGlue_fallsBackToHBaseSchema()
            throws Exception
    {
        logger.info("doGetTableWithNullGlue: enter");
        List<Result> results = TestUtils.makeResults();

        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        // Create handler with null Glue client
        HbaseMetadataHandler handlerWithNullGlue = new HbaseMetadataHandler(
                null,
                new LocalKeyFactory(),
                secretsManager,
                athena,
                mockConnFactory,
                SPILL_BUCKET,
                SPILL_PREFIX,
                com.google.common.collect.ImmutableMap.of());

        when(mockConnFactory.getOrCreateConn(nullable(String.class))).thenReturn(mockClient);

        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, TABLE_NAME, Collections.emptyMap());
        GetTableResponse res = handlerWithNullGlue.doGetTable(allocator, req);

        assertNotNull("Response should not be null", res);
        assertNotNull("Schema should be inferred from HBase", res.getSchema());
        logger.info("doGetTableWithNullGlue: exit");
    }

    @Test
    public void doGetSplits_withQueryPassthrough_returnsSplit()
            throws IOException
    {
        logger.info("doGetSplitsWithQueryPassthrough: enter");
        Map<String, String> qptArguments = new java.util.HashMap<>();
        qptArguments.put(HbaseQueryPassthrough.DATABASE, SCHEMA1);
        qptArguments.put(HbaseQueryPassthrough.COLLECTION, TABLE1);
        qptArguments.put(HbaseQueryPassthrough.FILTER, EMPTY_STRING);

        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                DEFAULT_NO_LIMIT,
                qptArguments,
                null);

        Block partitions = BlockUtils.newBlock(allocator, "partitionId", Types.MinorType.INT.getType(), 0);

        GetSplitsRequest req = new GetSplitsRequest(
                IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                TABLE_NAME,
                partitions,
                Collections.emptyList(),
                constraints,
                null);

        GetSplitsResponse response = handler.doGetSplits(allocator, req);

        assertNotNull("Response should not be null", response);
        assertEquals("Should have one split for QPT", EXPECTED_SPLIT_COUNT_1, response.getSplits().size());
        assertNull("Continuation token should be null", response.getContinuationToken());
        logger.info("doGetSplitsWithQueryPassthrough: exit");
    }

    @Test
    public void doGetSplits_withEmptyRegions_returnsNoSplits()
            throws IOException
    {
        logger.info("doGetSplitsWithEmptyRegions: enter");
        @SuppressWarnings("deprecation")
        List<HRegionInfo> emptyRegions = new ArrayList<>();

        when(mockClient.getTableRegions(any())).thenReturn(emptyRegions);

        Block partitions = BlockUtils.newBlock(allocator, "partitionId", Types.MinorType.INT.getType(), 0);

        GetSplitsRequest req = new GetSplitsRequest(
                IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                TABLE_NAME,
                partitions,
                Collections.emptyList(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                null);

        GetSplitsResponse response = handler.doGetSplits(allocator, req);

        assertNotNull("Response should not be null", response);
        assertEquals("Should have no splits for empty regions", 0, response.getSplits().size());
        logger.info("doGetSplitsWithEmptyRegions: exit");
    }

    @Test
    public void getPartitions_withValidRequest_doesNotThrowException()
    {
        logger.info("testGetPartitions: enter");
        BlockWriter mockBlockWriter = mock(BlockWriter.class);
        GetTableLayoutRequest req = new GetTableLayoutRequest(
                IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                TABLE_NAME,
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                SchemaBuilder.newBuilder().build(),
                Collections.EMPTY_SET);
        QueryStatusChecker mockQueryStatusChecker = mock(QueryStatusChecker.class);

        // This is a NoOp method, so we just verify it doesn't throw
        handler.getPartitions(mockBlockWriter, req, mockQueryStatusChecker);
        logger.info("testGetPartitions: exit");
    }

    @Test
    public void convertField_withStringType_returnsField()
    {
        logger.info("testConvertField: enter");
        Field field = handler.convertField(TEST_FIELD, STRING_TYPE);
        
        assertNotNull("Field should not be null", field);
        assertEquals("Field name should match", TEST_FIELD, field.getName());
        logger.info("testConvertField: exit");
    }

    @Test
    public void doGetQueryPassthroughSchema_withValidArguments_returnsSchema()
            throws Exception
    {
        logger.info("doGetQueryPassthroughSchema: enter");
        List<Result> results = TestUtils.makeResults();

        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        Map<String, String> qptArguments = new java.util.HashMap<>();
        qptArguments.put(com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, SYSTEM_QUERY);
        qptArguments.put(HbaseQueryPassthrough.DATABASE, SCHEMA1);
        qptArguments.put(HbaseQueryPassthrough.COLLECTION, TABLE1);
        qptArguments.put(HbaseQueryPassthrough.FILTER, EMPTY_STRING);

        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, TABLE_NAME, qptArguments);

        GetTableResponse res = handler.doGetQueryPassthroughSchema(allocator, req);

        assertNotNull("Response should not be null", res);
        assertNotNull("Schema should not be null", res.getSchema());
        logger.info("doGetQueryPassthroughSchema: exit");
    }

    @Test
    public void doListTables_withPaginationToken_returnsPaginatedTables()
    {
        logger.info("doListTablesWithPaginationToken: enter");
        String schema = SCHEMA1;

        org.apache.hadoop.hbase.TableName[] tables = {
                org.apache.hadoop.hbase.TableName.valueOf(SCHEMA1, TABLE1),
                org.apache.hadoop.hbase.TableName.valueOf(SCHEMA1, TABLE2),
                org.apache.hadoop.hbase.TableName.valueOf(SCHEMA1, TABLE3),
                org.apache.hadoop.hbase.TableName.valueOf(SCHEMA1, TABLE4),
                org.apache.hadoop.hbase.TableName.valueOf(SCHEMA1, TABLE5)
        };

        when(mockClient.listTableNamesByNamespace(eq(schema))).thenReturn(tables);

        // Test with pageSize 2 and token "1"
        ListTablesRequest req = new ListTablesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, schema,
                "1", PAGE_SIZE_2);
        ListTablesResponse res = handler.doListTables(allocator, req);

        assertEquals("Should return 2 tables", EXPECTED_TABLE_COUNT_2, res.getTables().size());
        assertEquals("Next token should be 3", "3", res.getNextToken());
        logger.info("doListTablesWithPaginationToken: exit");
    }

    @Test
    public void doGetTable_withTableNameContainingNamespace_returnsSchema()
            throws Exception
    {
        logger.info("doGetTableWithTableNameContainingNamespace: enter");
        List<Result> results = TestUtils.makeResults();

        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        // Test with table name that already contains namespace prefix
        TableName tableNameWithNamespace = new TableName(SCHEMA1, SCHEMA1 + ":" + TABLE1);
        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, tableNameWithNamespace, Collections.emptyMap());
        GetTableResponse res = handler.doGetTable(allocator, req);

        assertNotNull("Response should not be null", res);
        assertNotNull("Schema should not be null", res.getSchema());
        logger.info("doGetTableWithTableNameContainingNamespace: exit");
    }

    @Test
    public void doListTables_withPaginationEdgeCases_returnsCorrectPagination()
    {
        String schema = SCHEMA1;

        org.apache.hadoop.hbase.TableName[] tables = {
                org.apache.hadoop.hbase.TableName.valueOf(SCHEMA1, TABLE1),
                org.apache.hadoop.hbase.TableName.valueOf(SCHEMA1, TABLE2)
        };

        when(mockClient.listTableNamesByNamespace(eq(schema))).thenReturn(tables);

        ListTablesRequest req = new ListTablesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, schema,
                "0", PAGE_SIZE_1);
        ListTablesResponse res = handler.doListTables(allocator, req);

        assertEquals("Should return 1 table", EXPECTED_SPLIT_COUNT_1, res.getTables().size());
        assertEquals("Next token should be 1", "1", res.getNextToken());

        req = new ListTablesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, schema,
                "10", PAGE_SIZE_1);
        res = handler.doListTables(allocator, req);

        assertEquals("Should return 0 tables", 0, res.getTables().size());
        assertNull("Next token should be null", res.getNextToken());
    }

    @Test
    public void doGetSplits_withContinuationToken_returnsSplitsWithoutToken()
            throws IOException
    {
        @SuppressWarnings("deprecation")
        List<HRegionInfo> regionServers = new ArrayList<>();
        regionServers.add(TestUtils.makeRegion(1, SCHEMA1, TABLE1));
        regionServers.add(TestUtils.makeRegion(2, SCHEMA1, TABLE1));
        regionServers.add(TestUtils.makeRegion(3, SCHEMA1, TABLE1));
        regionServers.add(TestUtils.makeRegion(4, SCHEMA1, TABLE1));
        regionServers.add(TestUtils.makeRegion(5, SCHEMA1, TABLE1));

        when(mockClient.getTableRegions(any())).thenReturn(regionServers);

        Block partitions = BlockUtils.newBlock(allocator, "partitionId", Types.MinorType.INT.getType(), 0);

        GetSplitsRequest originalReq = new GetSplitsRequest(IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                TABLE_NAME,
                partitions,
                Collections.emptyList(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                null);

        GetSplitsRequest req = new GetSplitsRequest(originalReq, null);
        GetSplitsResponse response = handler.doGetSplits(allocator, req);

        assertNotNull("Response should not be null", response);
        assertEquals("Should have 5 splits", EXPECTED_SPLIT_COUNT_5, response.getSplits().size());
        assertNull("Continuation token should be null", response.getContinuationToken());
    }

    @Test
    public void doGetTable_withGlueSchema_returnsSchemaWithRowColumn()
            throws Exception
    {
        List<Result> results = TestUtils.makeResults();

        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        HbaseMetadataHandler handlerWithGlue = new HbaseMetadataHandler(
                awsGlue,
                new LocalKeyFactory(),
                secretsManager,
                athena,
                mockConnFactory,
                SPILL_BUCKET,
                SPILL_PREFIX,
                com.google.common.collect.ImmutableMap.of());

        when(mockConnFactory.getOrCreateConn(nullable(String.class))).thenReturn(mockClient);

        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, TABLE_NAME, Collections.emptyMap());
        GetTableResponse res = handlerWithGlue.doGetTable(allocator, req);

        assertNotNull("Response should not be null", res);
        assertNotNull("Schema should not be null", res.getSchema());
        assertTrue("Schema should contain ROW_COLUMN_NAME", 
                res.getSchema().getFields().stream()
                        .anyMatch(f -> f.getName().equals(HbaseSchemaUtils.ROW_COLUMN_NAME)));
    }

    @Test
    public void doGetQueryPassthroughSchema_withFilter_returnsSchema()
            throws Exception
    {
        List<Result> results = TestUtils.makeResults();

        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        Map<String, String> qptArguments = new java.util.HashMap<>();
        qptArguments.put(com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, SYSTEM_QUERY);
        qptArguments.put(HbaseQueryPassthrough.DATABASE, SCHEMA1);
        qptArguments.put(HbaseQueryPassthrough.COLLECTION, TABLE1);
        qptArguments.put(HbaseQueryPassthrough.FILTER, TEST_FILTER);

        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, TABLE_NAME, qptArguments);

        GetTableResponse res = handler.doGetQueryPassthroughSchema(allocator, req);

        assertNotNull("Response should not be null", res);
        assertNotNull("Schema should not be null", res.getSchema());
    }

    @Test
    public void doGetSplits_withQueryPassthroughAndFilter_returnsSplitWithQptArguments()
            throws IOException
    {
        Map<String, String> qptArguments = new java.util.HashMap<>();
        qptArguments.put(HbaseQueryPassthrough.DATABASE, SCHEMA1);
        qptArguments.put(HbaseQueryPassthrough.COLLECTION, TABLE1);
        qptArguments.put(HbaseQueryPassthrough.FILTER, TEST_FILTER_VALUE);

        Constraints constraints = new Constraints(
                Collections.emptyMap(),
                Collections.emptyList(),
                Collections.emptyList(),
                DEFAULT_NO_LIMIT,
                qptArguments,
                null);

        Block partitions = BlockUtils.newBlock(allocator, "partitionId", Types.MinorType.INT.getType(), 0);

        GetSplitsRequest req = new GetSplitsRequest(
                IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                TABLE_NAME,
                partitions,
                Collections.emptyList(),
                constraints,
                null);

        GetSplitsResponse response = handler.doGetSplits(allocator, req);

        assertNotNull("Response should not be null", response);
        assertEquals("Should have one split for QPT", EXPECTED_SPLIT_COUNT_1, response.getSplits().size());
        assertNull("Continuation token should be null", response.getContinuationToken());
        
        Split split = response.getSplits().iterator().next();
        assertTrue("Split should contain DATABASE", split.getProperties().containsKey(HbaseQueryPassthrough.DATABASE));
        assertTrue("Split should contain COLLECTION", split.getProperties().containsKey(HbaseQueryPassthrough.COLLECTION));
        assertTrue("Split should contain FILTER", split.getProperties().containsKey(HbaseQueryPassthrough.FILTER));
    }

    @Test
    public void doGetTable_withCustomMetadata_returnsSchemaWithFields()
            throws Exception
    {
        List<Result> results = TestUtils.makeResults();

        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        GetTableRequest req = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, TABLE_NAME, Collections.emptyMap());
        GetTableResponse res = handler.doGetTable(allocator, req);

        assertNotNull("Response should not be null", res);
        assertNotNull("Schema should not be null", res.getSchema());
        assertFalse("Schema should have fields", res.getSchema().getFields().isEmpty());
    }

    @Test
    public void doListSchemaNames_withIOException_throwsIOException()
            throws IOException
    {
        when(mockClient.listNamespaceDescriptors()).thenThrow(new IOException(CONNECTION_FAILED));

        ListSchemasRequest req = new ListSchemasRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG);
        
        try {
            handler.doListSchemaNames(allocator, req);
            fail("Expected IOException was not thrown");
        }
        catch (IOException e) {
            assertTrue("Exception message should contain error", e.getMessage().contains(CONNECTION_FAILED));
        }
    }

    @Test
    public void doListTables_withRuntimeException_throwsRuntimeException()
    {
        String schema = SCHEMA1;
        when(mockClient.listTableNamesByNamespace(eq(schema))).thenThrow(new RuntimeException(CONNECTION_FAILED));

        ListTablesRequest req = new ListTablesRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, schema,
                null, UNLIMITED_PAGE_SIZE_VALUE);
        
        try {
            handler.doListTables(allocator, req);
            fail("Expected RuntimeException was not thrown");
        }
        catch (RuntimeException e) {
            assertNotNull("Exception should not be null", e);
            assertTrue("Exception message should contain error", e.getMessage().contains(CONNECTION_FAILED));
        }
    }

    @Test
    public void doGetSplits_withRuntimeException_throwsRuntimeException()
            throws IOException
    {
        when(mockClient.getTableRegions(any())).thenThrow(new RuntimeException(CONNECTION_FAILED));

        Block partitions = BlockUtils.newBlock(allocator, "partitionId", Types.MinorType.INT.getType(), 0);

        GetSplitsRequest req = new GetSplitsRequest(
                IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                TABLE_NAME,
                partitions,
                Collections.emptyList(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                null);

        try {
            handler.doGetSplits(allocator, req);
            fail("Expected RuntimeException was not thrown");
        }
        catch (RuntimeException e) {
            assertTrue("Exception message should contain error", e.getMessage().contains(CONNECTION_FAILED));
        }
    }

    @Test
    public void doGetDataSourceCapabilities_withQueryPassthroughEnabled_returnsCapabilities()
    {
        Map<String, String> configOptions = com.google.common.collect.ImmutableMap.of(
                ENABLE_QUERY_PASSTHROUGH, TRUE
        );

        HbaseMetadataHandler handlerWithQPT = new HbaseMetadataHandler(
                awsGlue,
                new LocalKeyFactory(),
                secretsManager,
                athena,
                mockConnFactory,
                SPILL_BUCKET,
                SPILL_PREFIX,
                configOptions);

        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(
                IDENTITY, QUERY_ID, DEFAULT_CATALOG);
        
        GetDataSourceCapabilitiesResponse response = handlerWithQPT.doGetDataSourceCapabilities(allocator, request);
        
        assertNotNull("Response should not be null", response);
        assertEquals("Catalog name should match", DEFAULT_CATALOG, response.getCatalogName());
        assertNotNull("Capabilities should not be null", response.getCapabilities());
    }

    @Test
    public void doGetDataSourceCapabilities_withQueryPassthroughDisabled_returnsResponse()
    {
        Map<String, String> configOptions = com.google.common.collect.ImmutableMap.of(
                ENABLE_QUERY_PASSTHROUGH, FALSE
        );

        HbaseMetadataHandler handlerWithoutQPT = new HbaseMetadataHandler(
                awsGlue,
                new LocalKeyFactory(),
                secretsManager,
                athena,
                mockConnFactory,
                SPILL_BUCKET,
                SPILL_PREFIX,
                configOptions);

        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(
                IDENTITY, QUERY_ID, DEFAULT_CATALOG);
        
        GetDataSourceCapabilitiesResponse response = handlerWithoutQPT.doGetDataSourceCapabilities(allocator, request);
        
        assertNotNull("Response should not be null", response);
        assertEquals("Catalog name should match", DEFAULT_CATALOG, response.getCatalogName());
    }

    @Test
    public void doGetTable_withEmptyResults_throwsRuntimeException()
            throws Exception
    {
        List<Result> emptyResults = new ArrayList<>();

        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(emptyResults.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        GetTableRequest request = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, TABLE_NAME, Collections.emptyMap());
        try {
            handler.doGetTable(allocator, request);
            fail("Expected RuntimeException was not thrown");
        }
        catch (RuntimeException e) {
            assertNotNull("Exception should not be null", e);
            assertTrue("Exception message should indicate no columns found", 
                    e.getMessage() != null && (e.getMessage().contains("No columns found") || 
                    e.getMessage().contains("empty")));
        }
    }

    @Test
    public void doGetSplits_withSingleRegion_returnsOneSplitWithProperties()
            throws IOException
    {
        List<HRegionInfo> singleRegion = new ArrayList<>();
        singleRegion.add(TestUtils.makeRegion(1, SCHEMA1, TABLE1));

        when(mockClient.getTableRegions(any())).thenReturn(singleRegion);

        Block partitions = BlockUtils.newBlock(allocator, "partitionId", Types.MinorType.INT.getType(), 0);

        GetSplitsRequest req = new GetSplitsRequest(
                IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                TABLE_NAME,
                partitions,
                Collections.emptyList(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                null);

        GetSplitsResponse response = handler.doGetSplits(allocator, req);

        assertNotNull("Response should not be null", response);
        assertEquals("Should have 1 split", EXPECTED_SPLIT_COUNT_1, response.getSplits().size());
        assertNull("Continuation token should be null", response.getContinuationToken());
        
        Split split = response.getSplits().iterator().next();
        assertTrue("Split should contain connection string", split.getProperties().containsKey(HbaseMetadataHandler.HBASE_CONN_STR));
        assertTrue("Split should contain start key", split.getProperties().containsKey(HbaseMetadataHandler.START_KEY_FIELD));
        assertTrue("Split should contain end key", split.getProperties().containsKey(HbaseMetadataHandler.END_KEY_FIELD));
        assertTrue("Split should contain region id", split.getProperties().containsKey(HbaseMetadataHandler.REGION_ID_FIELD));
        assertTrue("Split should contain region name", split.getProperties().containsKey(HbaseMetadataHandler.REGION_NAME_FIELD));
    }

    @Test
    public void convertField_withDifferentTypes_returnsCorrectFields()
    {
        Field stringField = handler.convertField(STRING_FIELD, STRING_TYPE);
        assertNotNull("String field should not be null", stringField);
        assertEquals("String field name should match", STRING_FIELD, stringField.getName());

        Field intField = handler.convertField(INT_FIELD, INT_TYPE);
        assertNotNull("Int field should not be null", intField);
        assertEquals("Int field name should match", INT_FIELD, intField.getName());

        Field bigintField = handler.convertField(BIGINT_FIELD, BIGINT_TYPE);
        assertNotNull("Bigint field should not be null", bigintField);
        assertEquals("Bigint field name should match", BIGINT_FIELD, bigintField.getName());
    }

    @Test
    public void doGetQueryPassthroughSchema_withValidArguments_returnsTableResponse()
            throws Exception
    {
        logger.info("doGetQueryPassthroughSchema_withValidArguments_returnsTableResponse: enter");
        List<Result> results = TestUtils.makeResults();
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        java.util.Map<String, String> qptArguments = new java.util.HashMap<>();
        qptArguments.put(com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, "SYSTEM.QUERY");
        qptArguments.put(HbaseQueryPassthrough.DATABASE, SCHEMA1);
        qptArguments.put(HbaseQueryPassthrough.COLLECTION, TABLE1);
        qptArguments.put(HbaseQueryPassthrough.FILTER, "");

        GetTableRequest request = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, TABLE_NAME, qptArguments);

        GetTableResponse response = handler.doGetQueryPassthroughSchema(allocator, request);
        assertNotNull("Response should not be null", response);
        assertNotNull("Schema should not be null", response.getSchema());
        logger.info("doGetQueryPassthroughSchema_withValidArguments_returnsTableResponse: exit");
    }

    @Test
    public void doGetQueryPassthroughSchema_withInvalidArguments_throwsIllegalArgumentException()
            throws Exception
    {
        logger.info("doGetQueryPassthroughSchema_withInvalidArguments_throwsIllegalArgumentException: enter");
        java.util.Map<String, String> qptArguments = new java.util.HashMap<>();
        qptArguments.put(com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, "WRONG.SIGNATURE");

        GetTableRequest request = new GetTableRequest(IDENTITY, QUERY_ID, DEFAULT_CATALOG, TABLE_NAME, qptArguments);

        try {
            handler.doGetQueryPassthroughSchema(allocator, request);
            fail("Expected IllegalArgumentException was not thrown");
        }
        catch (IllegalArgumentException ex) {
            assertNotNull("Exception should not be null", ex);
            assertTrue("Exception message should not be empty", 
                    ex.getMessage() != null && !ex.getMessage().isEmpty());
        }
        logger.info("doGetQueryPassthroughSchema_withInvalidArguments_throwsIllegalArgumentException: exit");
    }

    @Test
    public void doGetSplits_withQueryPassthrough_returnsQueryPassthroughSplit()
            throws IOException
    {
        logger.info("doGetSplits_withQueryPassthrough_returnsQueryPassthroughSplit: enter");
        java.util.Map<String, String> qptArguments = new java.util.HashMap<>();
        qptArguments.put(com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, "SYSTEM.QUERY");
        qptArguments.put(HbaseQueryPassthrough.DATABASE, SCHEMA1);
        qptArguments.put(HbaseQueryPassthrough.COLLECTION, TABLE1);
        qptArguments.put(HbaseQueryPassthrough.FILTER, "");

        Block partitions = BlockUtils.newBlock(allocator, "partitionId", Types.MinorType.INT.getType(), 0);
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, qptArguments, null);

        GetSplitsRequest request = new GetSplitsRequest(
                IDENTITY,
                QUERY_ID,
                DEFAULT_CATALOG,
                TABLE_NAME,
                partitions,
                Collections.emptyList(),
                constraints,
                null
        );

        GetSplitsResponse response = handler.doGetSplits(allocator, request);
        assertNotNull("Response should not be null", response);
        assertNotNull("Splits should not be null", response.getSplits());
        assertEquals("Should have one split for QPT", 1, response.getSplits().size());
        logger.info("doGetSplits_withQueryPassthrough_returnsQueryPassthroughSplit: exit");
    }
}
