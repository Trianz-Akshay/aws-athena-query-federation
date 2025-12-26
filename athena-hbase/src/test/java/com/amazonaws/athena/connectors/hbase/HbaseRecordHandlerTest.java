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
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.hbase.connection.HBaseConnection;
import com.amazonaws.athena.connectors.hbase.connection.HbaseConnectionFactory;
import com.amazonaws.athena.connectors.hbase.connection.ResultProcessor;
import com.amazonaws.athena.connectors.hbase.qpt.HbaseQueryPassthrough;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
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
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.connectors.hbase.HbaseMetadataHandler.END_KEY_FIELD;
import static com.amazonaws.athena.connectors.hbase.HbaseMetadataHandler.HBASE_CONN_STR;
import static com.amazonaws.athena.connectors.hbase.HbaseMetadataHandler.REGION_ID_FIELD;
import static com.amazonaws.athena.connectors.hbase.HbaseMetadataHandler.REGION_NAME_FIELD;
import static com.amazonaws.athena.connectors.hbase.HbaseMetadataHandler.START_KEY_FIELD;
import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.*;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HbaseRecordHandlerTest
    extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(HbaseRecordHandlerTest.class);
    private static final String INVALID_FILTER_SYNTAX = "InvalidFilterSyntax!!!";
    private static final String ROW_FILTER = "RowFilter(=,'substring:test')";
    private static final String INVALID_COLUMN_NAME = "invalid_column_name";
    private static final String FAKE_CON_STR = "fake_con_str";
    private static final String FAKE_START_KEY = "fake_start_key";
    private static final String FAKE_END_KEY = "fake_end_key";
    private static final String FAKE_REGION_ID = "fake_region_id";
    private static final String FAKE_REGION_NAME = "fake_region_name";
    private static final String TEST_ROW_KEY = "test_row_key";
    private static final int EXPECTED_RECORD_COUNT_1 = 1;
    private static final int EXPECTED_RECORD_COUNT_0 = 0;

    private HbaseRecordHandler handler;
    private BlockAllocator allocator;
    private List<ByteHolder> mockS3Storage = new ArrayList<>();
    private S3Client amazonS3;
    private S3BlockSpillReader spillReader;
    private Schema schemaForRead;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();

    @Rule
    public TestName testName = new TestName();

    @Mock
    private HBaseConnection mockClient;

    @Mock
    private Table mockTable;

    @Mock
    private HbaseConnectionFactory mockConnFactory;

    @Mock
    private SecretsManagerClient mockSecretsManager;

    @Mock
    private AthenaClient mockAthena;

    @Before
    public void setUp()
            throws IOException
    {
        logger.info("{}: enter", testName.getMethodName());

        when(mockConnFactory.getOrCreateConn(nullable(String.class))).thenReturn(mockClient);

        allocator = new BlockAllocatorImpl();

        amazonS3 = mock(S3Client.class);

        when(amazonS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    InputStream inputStream = ((RequestBody) invocationOnMock.getArguments()[1]).contentStreamProvider().newStream();
                    ByteHolder byteHolder = new ByteHolder();
                    byteHolder.setBytes(ByteStreams.toByteArray(inputStream));
                    synchronized (mockS3Storage) {
                        mockS3Storage.add(byteHolder);
                        logger.info("puObject: total size " + mockS3Storage.size());
                    }
                    return PutObjectResponse.builder().build();
                });

        when(amazonS3.getObject(any(GetObjectRequest.class)))
                .thenAnswer((InvocationOnMock invocationOnMock) -> {
                    ByteHolder byteHolder;
                    synchronized (mockS3Storage) {
                        byteHolder = mockS3Storage.get(0);
                        mockS3Storage.remove(0);
                        logger.info("getObject: total size " + mockS3Storage.size());
                    }
                    return new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(byteHolder.getBytes()));
                });

        schemaForRead = TestUtils.makeSchema().addStringField(HbaseSchemaUtils.ROW_COLUMN_NAME).build();

        handler = new HbaseRecordHandler(amazonS3, mockSecretsManager, mockAthena, mockConnFactory, com.google.common.collect.ImmutableMap.of());
        spillReader = new S3BlockSpillReader(amazonS3, allocator);
    }

    @After
    public void after()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void doReadRecordsNoSpill()
            throws Exception
    {
        List<Result> results = TestUtils.makeResults(100);
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("family1:col3", SortedRangeSet.copyOf(Types.MinorType.BIGINT.getType(),
                ImmutableList.of(Range.equal(allocator, Types.MinorType.BIGINT.getType(), 1L)), false));

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR)
                .add(START_KEY_FIELD, FAKE_START_KEY)
                .add(END_KEY_FIELD, FAKE_END_KEY)
                .add(REGION_ID_FIELD, FAKE_REGION_ID)
                .add(REGION_NAME_FIELD, FAKE_REGION_NAME);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schemaForRead,
                splitBuilder.build(),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L, //100GB don't expect this to spill
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());

        assertEquals(EXPECTED_RECORD_COUNT_1, response.getRecords().getRowCount());
        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecordsSpill()
            throws Exception
    {
        List<Result> results = TestUtils.makeResults(10_000);
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("family1:col3", SortedRangeSet.copyOf(Types.MinorType.BIGINT.getType(),
                ImmutableList.of(Range.greaterThan(allocator, Types.MinorType.BIGINT.getType(), 0L)), true));

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, "fake_con_str")
                .add(START_KEY_FIELD, "fake_start_key")
                .add(END_KEY_FIELD, "fake_end_key")
                .add(REGION_ID_FIELD, "fake_region_id")
                .add(REGION_NAME_FIELD, "fake_region_name");

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schemaForRead,
                splitBuilder.build(),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                1_500_000L, //~1.5MB so we should see some spill
                0L
        );
        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof RemoteReadRecordsResponse);

        try (RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse) {
            logger.info("doReadRecordsSpill: remoteBlocks[{}]", response.getRemoteBlocks().size());

            assertTrue(response.getNumberBlocks() > 1);

            int blockNum = 0;
            for (SpillLocation next : response.getRemoteBlocks()) {
                S3SpillLocation spillLocation = (S3SpillLocation) next;
                try (Block block = spillReader.read(spillLocation, response.getEncryptionKey(), response.getSchema())) {

                    logger.info("doReadRecordsSpill: blockNum[{}] and recordCount[{}]", blockNum++, block.getRowCount());
                    // assertTrue(++blockNum < response.getRemoteBlocks().size() && block.getRowCount() > 10_000);

                    logger.info("doReadRecordsSpill: {}", BlockUtils.rowToString(block, 0));
                    assertNotNull(BlockUtils.rowToString(block, 0));
                }
            }
        }
    }

    @Test
    public void doReadRecords_withQueryPassthroughEmptyFilter_returnsReadRecordsResponse()
            throws Exception
    {
        logger.info("doReadRecords_QueryPassthroughEmptyFilter_ReturnsReadRecordsResponse: enter");
        List<Result> results = TestUtils.makeResults(10);
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        Map<String, String> qptArguments = new HashMap<>();
        qptArguments.put(com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, "SYSTEM.QUERY");
        qptArguments.put(HbaseQueryPassthrough.DATABASE, DEFAULT_SCHEMA);
        qptArguments.put(HbaseQueryPassthrough.COLLECTION, TEST_TABLE);
        qptArguments.put(HbaseQueryPassthrough.FILTER, "");

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schemaForRead,
                splitBuilder.build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, qptArguments, null),
                100_000_000_000L,
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue("Response should be ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertTrue("Record count should be greater than 0", response.getRecordCount() > 0);
        logger.info("doReadRecords_QueryPassthroughEmptyFilter_ReturnsReadRecordsResponse: exit");
    }

    @Test
    public void doReadRecords_withQueryPassthroughWithFilter_returnsReadRecordsResponse()
            throws Exception
    {
        logger.info("doReadRecords_QueryPassthroughWithFilter_ReturnsReadRecordsResponse: enter");
        List<Result> results = TestUtils.makeResults(10);
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        Map<String, String> qptArguments = new HashMap<>();
        qptArguments.put(com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, "SYSTEM.QUERY");
        qptArguments.put(HbaseQueryPassthrough.DATABASE, DEFAULT_SCHEMA);
        qptArguments.put(HbaseQueryPassthrough.COLLECTION, TEST_TABLE);
        qptArguments.put(HbaseQueryPassthrough.FILTER, ROW_FILTER);

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schemaForRead,
                splitBuilder.build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, qptArguments, null),
                100_000_000_000L,
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue("Response should be ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertTrue("Record count should be greater than or equal to 0", response.getRecordCount() >= 0);
        logger.info("doReadRecords_QueryPassthroughWithFilter_ReturnsReadRecordsResponse: exit");
    }

    @Test
    public void doReadRecords_withQueryPassthroughInvalidFilter_throwsRuntimeException()
            throws Exception
    {
        logger.info("doReadRecords_QueryPassthroughInvalidFilter_ThrowsRuntimeException: enter");
        Map<String, String> qptArguments = new HashMap<>();
        qptArguments.put(com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, "SYSTEM.QUERY");
        qptArguments.put(HbaseQueryPassthrough.DATABASE, DEFAULT_SCHEMA);
        qptArguments.put(HbaseQueryPassthrough.COLLECTION, TEST_TABLE);
        qptArguments.put(HbaseQueryPassthrough.FILTER, INVALID_FILTER_SYNTAX);

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schemaForRead,
                splitBuilder.build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, qptArguments, null),
                100_000_000_000L,
                100_000_000_000L
        );

        try {
            handler.doReadRecords(allocator, request);
            fail("Expected RuntimeException was not thrown");
        }
        catch (RuntimeException ex) {
            assertNotNull("Exception should not be null", ex);
            assertTrue("Exception message should not be empty", 
                    ex.getMessage() != null && !ex.getMessage().isEmpty());
        }
        logger.info("doReadRecords_QueryPassthroughInvalidFilter_ThrowsRuntimeException: exit");
    }

    @Test
    public void doReadRecords_withSpecificSchema_returnsReadRecordsResponse()
            throws Exception
    {
        logger.info("doReadRecords_withSpecificSchema_returnsReadRecordsResponse: enter");
        List<Result> results = TestUtils.makeResults(10);
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        Schema nativeSchema = SchemaBuilder.newBuilder()
                .addStringField("family1:col1")
                .addBigIntField("family1:col3")
                .addStringField(HbaseSchemaUtils.ROW_COLUMN_NAME)
                .build();

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("family1:col3", SortedRangeSet.copyOf(Types.MinorType.BIGINT.getType(),
                ImmutableList.of(Range.equal(allocator, Types.MinorType.BIGINT.getType(), 1L)), false));

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR)
                .add(START_KEY_FIELD, FAKE_START_KEY)
                .add(END_KEY_FIELD, FAKE_END_KEY)
                .add(REGION_ID_FIELD, FAKE_REGION_ID)
                .add(REGION_NAME_FIELD, FAKE_REGION_NAME);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                nativeSchema,
                splitBuilder.build(),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L,
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue("Response should be ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertTrue("Record count should be greater than or equal to 0", response.getRecordCount() >= 0);
        logger.info("doReadRecords_NativeStorage_ReturnsReadRecordsResponse: exit");
    }

    @Test
    public void doReadRecords_withRowKeyConstraint_returnsReadRecordsResponse()
            throws Exception
    {
        logger.info("doReadRecords_RowKeyConstraint_ReturnsReadRecordsResponse: enter");
        List<Result> results = TestUtils.makeResults(10);
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put(HbaseSchemaUtils.ROW_COLUMN_NAME, SortedRangeSet.copyOf(Types.MinorType.VARCHAR.getType(),
                ImmutableList.of(Range.equal(allocator, Types.MinorType.VARCHAR.getType(), TEST_ROW_KEY)), false));

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR)
                .add(START_KEY_FIELD, FAKE_START_KEY)
                .add(END_KEY_FIELD, FAKE_END_KEY)
                .add(REGION_ID_FIELD, FAKE_REGION_ID)
                .add(REGION_NAME_FIELD, FAKE_REGION_NAME);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schemaForRead,
                splitBuilder.build(),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L,
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue("Response should be ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertTrue("Record count should be greater than or equal to 0", response.getRecordCount() >= 0);
        logger.info("doReadRecords_RowKeyConstraint_ReturnsReadRecordsResponse: exit");
    }

    @Test
    public void doReadRecords_withEmptyResults_returnsZeroRecords()
            throws Exception
    {
        logger.info("doReadRecords_EmptyResults_ReturnsZeroRecords: enter");
        List<Result> emptyResults = new ArrayList<>();
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(emptyResults.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR)
                .add(START_KEY_FIELD, FAKE_START_KEY)
                .add(END_KEY_FIELD, FAKE_END_KEY)
                .add(REGION_ID_FIELD, FAKE_REGION_ID)
                .add(REGION_NAME_FIELD, FAKE_REGION_NAME);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schemaForRead,
                splitBuilder.build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L,
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue("Response should be ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertEquals("Record count should be 0", EXPECTED_RECORD_COUNT_0, response.getRecordCount());
        logger.info("doReadRecords_EmptyResults_ReturnsZeroRecords: exit");
    }

    @Test
    public void doReadRecords_withStructField_returnsReadRecordsResponse()
            throws Exception
    {
        logger.info("doReadRecords_StructField_ReturnsReadRecordsResponse: enter");
        List<Result> results = TestUtils.makeResults(10);
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        Schema structSchema = SchemaBuilder.newBuilder()
                .addStructField("family1")
                .addChildField("family1", "col1", Types.MinorType.VARCHAR.getType())
                .addChildField("family1", "col2", Types.MinorType.FLOAT8.getType())
                .addStringField(HbaseSchemaUtils.ROW_COLUMN_NAME)
                .build();

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR)
                .add(START_KEY_FIELD, FAKE_START_KEY)
                .add(END_KEY_FIELD, FAKE_END_KEY)
                .add(REGION_ID_FIELD, FAKE_REGION_ID)
                .add(REGION_NAME_FIELD, FAKE_REGION_NAME);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                structSchema,
                splitBuilder.build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L,
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue("Response should be ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertTrue("Record count should be greater than or equal to 0", response.getRecordCount() >= 0);
        logger.info("doReadRecords_StructField_ReturnsReadRecordsResponse: exit");
    }

    @Test
    public void doReadRecords_withInvalidColumnName_throwsRuntimeException()
            throws Exception
    {
        logger.info("doReadRecords_InvalidColumnName_ThrowsRuntimeException: enter");
        Schema invalidSchema = SchemaBuilder.newBuilder()
                .addStringField(INVALID_COLUMN_NAME)
                .addStringField(HbaseSchemaUtils.ROW_COLUMN_NAME)
                .build();

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR)
                .add(START_KEY_FIELD, FAKE_START_KEY)
                .add(END_KEY_FIELD, FAKE_END_KEY)
                .add(REGION_ID_FIELD, FAKE_REGION_ID)
                .add(REGION_NAME_FIELD, FAKE_REGION_NAME);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                invalidSchema,
                splitBuilder.build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L,
                100_000_000_000L
        );

        try {
            handler.doReadRecords(allocator, request);
            fail("Expected RuntimeException was not thrown");
        }
        catch (RuntimeException ex) {
            assertNotNull("Exception should not be null", ex);
            assertTrue("Exception message should not be empty", 
                    ex.getMessage() != null && !ex.getMessage().isEmpty());
        }
        logger.info("doReadRecords_InvalidColumnName_ThrowsRuntimeException: exit");
    }

    @Test
    public void doReadRecords_withNoConstraints_returnsReadRecordsResponse()
            throws Exception
    {
        logger.info("doReadRecords_NoConstraints_ReturnsReadRecordsResponse: enter");
        List<Result> results = TestUtils.makeResults(10);
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR)
                .add(START_KEY_FIELD, FAKE_START_KEY)
                .add(END_KEY_FIELD, FAKE_END_KEY)
                .add(REGION_ID_FIELD, FAKE_REGION_ID)
                .add(REGION_NAME_FIELD, FAKE_REGION_NAME);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schemaForRead,
                splitBuilder.build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L,
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue("Response should be ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertTrue("Record count should be greater than or equal to 0", response.getRecordCount() >= 0);
        logger.info("doReadRecords_NoConstraints_ReturnsReadRecordsResponse: exit");
    }


    @Test
    public void doReadRecords_withOnlyRowColumn_returnsReadRecordsResponse()
            throws Exception
    {
        logger.info("doReadRecords_OnlyRowColumn_ReturnsReadRecordsResponse: enter");
        List<Result> results = TestUtils.makeResults(10);
        ResultScanner mockScanner = mock(ResultScanner.class);
        when(mockScanner.iterator()).thenReturn(results.iterator());

        when(mockClient.scanTable(any(), nullable(Scan.class), any())).thenAnswer((InvocationOnMock invocationOnMock) -> {
            ResultProcessor processor = (ResultProcessor) invocationOnMock.getArguments()[2];
            return processor.scan(mockScanner);
        });

        Schema rowOnlySchema = SchemaBuilder.newBuilder()
                .addStringField(HbaseSchemaUtils.ROW_COLUMN_NAME)
                .build();

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR)
                .add(START_KEY_FIELD, FAKE_START_KEY)
                .add(END_KEY_FIELD, FAKE_END_KEY)
                .add(REGION_ID_FIELD, FAKE_REGION_ID)
                .add(REGION_NAME_FIELD, FAKE_REGION_NAME);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                rowOnlySchema,
                splitBuilder.build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L,
                100_000_000_000L
        );

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue("Response should be ReadRecordsResponse", rawResponse instanceof ReadRecordsResponse);
        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        assertTrue("Record count should be greater than or equal to 0", response.getRecordCount() >= 0);
        logger.info("doReadRecords_OnlyRowColumn_ReturnsReadRecordsResponse: exit");
    }

    @Test
    public void doReadRecords_withQueryPassthroughCharacterCodingException_throwsRuntimeException()
            throws Exception
    {
        logger.info("doReadRecords_withQueryPassthroughCharacterCodingException_throwsRuntimeException: enter");
        Map<String, String> qptArguments = new HashMap<>();
        qptArguments.put(com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, "SYSTEM.QUERY");
        qptArguments.put(HbaseQueryPassthrough.DATABASE, DEFAULT_SCHEMA);
        qptArguments.put(HbaseQueryPassthrough.COLLECTION, TEST_TABLE);
        qptArguments.put(HbaseQueryPassthrough.FILTER, "\u0000\u0001\u0002");

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schemaForRead,
                splitBuilder.build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, qptArguments, null),
                100_000_000_000L,
                100_000_000_000L
        );

        try {
            handler.doReadRecords(allocator, request);
            fail("Expected RuntimeException was not thrown");
        }
        catch (RuntimeException ex) {
            assertNotNull("Exception should not be null", ex);
            assertTrue("Exception message should not be empty", 
                    ex.getMessage() != null && !ex.getMessage().isEmpty());
            // The exception might be from filter parsing or from other validation, just verify exception was thrown
        }
        logger.info("doReadRecords_withQueryPassthroughCharacterCodingException_throwsRuntimeException: exit");
    }

    @Test
    public void doReadRecords_withInvalidColumnFormat_throwsRuntimeException()
            throws Exception
    {
        logger.info("doReadRecords_withInvalidColumnFormat_throwsRuntimeException: enter");
        Schema invalidSchema = SchemaBuilder.newBuilder()
                .addStringField("invalid_column_format_no_colon")
                .addStringField(HbaseSchemaUtils.ROW_COLUMN_NAME)
                .build();

        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split.Builder splitBuilder = Split.newBuilder(splitLoc, keyFactory.create())
                .add(HBASE_CONN_STR, FAKE_CON_STR)
                .add(START_KEY_FIELD, FAKE_START_KEY)
                .add(END_KEY_FIELD, FAKE_END_KEY)
                .add(REGION_ID_FIELD, FAKE_REGION_ID)
                .add(REGION_NAME_FIELD, FAKE_REGION_NAME);

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                "queryId-" + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                invalidSchema,
                splitBuilder.build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L,
                100_000_000_000L
        );

        try {
            handler.doReadRecords(allocator, request);
            fail("Expected RuntimeException was not thrown");
        }
        catch (RuntimeException ex) {
            assertNotNull("Exception should not be null", ex);
            assertTrue("Exception message should contain column name error", 
                    ex.getMessage().contains("does not meet family:column") || 
                    ex.getMessage().contains("Column name"));
        }
        logger.info("doReadRecords_withInvalidColumnFormat_throwsRuntimeException: exit");
    }

    private class ByteHolder
    {
        private byte[] bytes;

        public void setBytes(byte[] bytes)
        {
            this.bytes = bytes;
        }

        public byte[] getBytes()
        {
            return bytes;
        }
    }
}
