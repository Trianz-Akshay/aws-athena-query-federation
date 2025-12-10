/*-
 * #%L
 * athena-timestream
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena.connectors.timestream;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.timestream.qpt.TimestreamQueryPassthrough;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient;
import software.amazon.awssdk.services.timestreamquery.model.Datum;
import software.amazon.awssdk.services.timestreamquery.model.QueryRequest;
import software.amazon.awssdk.services.timestreamquery.model.QueryResponse;
import software.amazon.awssdk.services.timestreamquery.model.Row;
import software.amazon.awssdk.services.timestreamquery.model.TimeSeriesDataPoint;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.VIEW_METADATA_FIELD;
import static com.amazonaws.athena.connectors.timestream.TestUtils.makeMockQueryResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TimestreamRecordHandlerTest
{
    private static final Logger logger = LoggerFactory.getLogger(TimestreamRecordHandlerTest.class);
    private static final FederatedIdentity IDENTITY = new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
    private TimestreamRecordHandler handler;
    private BlockAllocator allocator;
    private List<ByteHolder> mockS3Storage = new ArrayList<>();
    private S3Client amazonS3;
    private S3BlockSpillReader spillReader;
    private Schema schemaForRead;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private static final String DEFAULT_CATALOG = "my_catalog";
    private static final String DEFAULT_SCHEMA = "my_schema";
    private static final String TEST_TABLE = "my_table";
    private static final String TEST_VIEW = "my_view";
    private static final String QUERY_ID_PREFIX = "queryId-";
    private static final String SYSTEM_QUERY_FUNCTION = "SYSTEM.QUERY";
    private static final String PASSTHROUGH_QUERY = "SELECT * FROM my_table WHERE col1 = 'value'";
    private static final String EXPECTED_QUERY_PAGINATION = "SELECT measure_name, measure_value::double, az, time, hostname, region FROM \"my_schema\".\"my_table\"";
    private static final String US_EAST_1A = "us-east-1a";
    private static final String REGION_1 = "region1";
    private static final String MEASURE_PREFIX = "measure_";
    private static final String HOST_PREFIX = "host";
    private static final String PAGINATION_TOKEN_PREFIX = "token";
    private static final String MEASURE_1 = "measure1";
    private static final String FLOAT_VALUE_1_5 = "1.5";
    private static final String HOST_1 = "host1";
    private static final String TRUE_VALUE = "true";
    private static final String BIGINT_VALUE = "123456789";
    private static final String COLUMN_NAME_1 = "col1";
    private static final String COLUMN_NAME_2 = "col2";
    private static final String COLUMN_NAME_3 = "col3";
    private static final String COLUMN_NAME_4 = "col4";
    private static final String TIMESTAMP_2024_01_01 = "2024-01-01 00:00:00.000";
    private static final String UNSUPPORTED_FIELD_TYPE = "Unsupported field type";

    @Rule
    public TestName testName = new TestName();

    @Mock
    private TimestreamQueryClient mockClient;

    @Mock
    private SecretsManagerClient mockSecretsManager;

    @Mock
    private AthenaClient mockAthena;

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

    @Before
    public void setUp()
            throws IOException
    {
        logger.info("{}: enter", testName.getMethodName());

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

        schemaForRead = SchemaBuilder.newBuilder()
                .addField("measure_name", Types.MinorType.VARCHAR.getType())
                .addField("measure_value::double", Types.MinorType.FLOAT8.getType())
                .addField("az", Types.MinorType.VARCHAR.getType())
                .addField("time", Types.MinorType.DATEMILLI.getType())
                .addField("hostname", Types.MinorType.VARCHAR.getType())
                .addField("region", Types.MinorType.VARCHAR.getType())
                .build();

        handler = new TimestreamRecordHandler(amazonS3, mockSecretsManager, mockAthena, mockClient, com.google.common.collect.ImmutableMap.of());
        spillReader = new S3BlockSpillReader(amazonS3, allocator);
    }

    @After
    public void after()
    {
        if (allocator != null) {
            allocator.close();
        }
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Test
    public void doReadRecordsNoSpill()
            throws Exception
    {
        int numRowsGenerated = 1_000;
        String expectedQuery = "SELECT measure_name, measure_value::double, az, time, hostname, region FROM \"my_schema\".\"my_table\" WHERE (\"az\" IN ('us-east-1a','us-east-1b'))";

        QueryResponse mockResult = makeMockQueryResult(schemaForRead, numRowsGenerated);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals(normalizeQuery(expectedQuery), normalizeQuery(request.queryString()));
                            return mockResult;
                        }
                );

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("az", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("us-east-1a")
                .add("us-east-1b").build());

        ReadRecordsRequest request = createReadRecordsRequestWithConstraints(schemaForRead, constraintsMap, 100_000_000_000L, 100_000_000_000L);
        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());

        assertTrue(response.getRecords().getRowCount() > 0);

        //ensure we actually filtered something out
        assertTrue(response.getRecords().getRowCount() < numRowsGenerated);

        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecordsSpill()
            throws Exception
    {
        String expectedQuery = "SELECT measure_name, measure_value::double, az, time, hostname, region FROM \"my_schema\".\"my_table\" WHERE (\"az\" IN ('us-east-1a','us-east-1b'))";

        QueryResponse mockResult = makeMockQueryResult(schemaForRead, 100_000);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals(normalizeQuery(expectedQuery), normalizeQuery(request.queryString()));
                            return mockResult;
                        }
                );

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("az", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("us-east-1a")
                .add("us-east-1b").build());

        ReadRecordsRequest request = createReadRecordsRequestWithConstraints(schemaForRead, constraintsMap, 1_500_000L, 0L);
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
    public void readRecordsView()
            throws Exception
    {
        logger.info("readRecordsView - enter");

        Schema schemaForReadView = SchemaBuilder.newBuilder()
                .addField("measure_name", Types.MinorType.VARCHAR.getType())
                .addField("az", Types.MinorType.VARCHAR.getType())
                .addField("value", Types.MinorType.FLOAT8.getType())
                .addField("num_samples", Types.MinorType.BIGINT.getType())
                .addMetadata(VIEW_METADATA_FIELD, "select measure_name, az,sum(\"measure_value::double\") as value, count(*) as num_samples from \"" +
                        DEFAULT_SCHEMA + "\".\"" + TEST_TABLE + "\" group by measure_name, az")
                .build();

        String expectedQuery = "WITH t1 AS ( select measure_name, az,sum(\"measure_value::double\") as value, count(*) as num_samples from \"my_schema\".\"my_table\" group by measure_name, az )  SELECT measure_name, az, value, num_samples FROM t1 WHERE (\"az\" IN ('us-east-1a','us-east-1b'))";

        QueryResponse mockResult = makeMockQueryResult(schemaForReadView, 1_000);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals(normalizeQuery(expectedQuery), normalizeQuery(request.queryString()));
                            return mockResult;
                        }
                );

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("az", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("us-east-1a")
                .add("us-east-1b").build());

        ReadRecordsRequest request = createReadRecordsRequestWithConstraints(schemaForReadView, "default", TEST_VIEW, constraintsMap, true);
        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("readRecordsView: rows[{}]", response.getRecordCount());

        for (int i = 0; i < response.getRecordCount() && i < 10; i++) {
            logger.info("readRecordsView: {}", BlockUtils.rowToString(response.getRecords(), i));
        }

        logger.info("readRecordsView - exit");
    }

    @Test
    public void readRecordsTimeSeriesView()
            throws Exception
    {
        logger.info("readRecordsTimeSeriesView - enter");

        Schema schemaForReadView = SchemaBuilder.newBuilder()
                .addField("region", Types.MinorType.VARCHAR.getType())
                .addField("az", Types.MinorType.VARCHAR.getType())
                .addField("hostname", Types.MinorType.VARCHAR.getType())
                .addField(FieldBuilder.newBuilder("cpu_utilization", Types.MinorType.LIST.getType())
                        .addField(FieldBuilder.newBuilder("cpu_utilization", Types.MinorType.STRUCT.getType())
                                .addDateMilliField("time")
                                .addFloat8Field("measure_value::double")
                                .build())
                        .build())
                .addMetadata(VIEW_METADATA_FIELD, "select az, hostname, region,  CREATE_TIME_SERIES(time, measure_value::double) as cpu_utilization from \"" + DEFAULT_SCHEMA + "\".\"" + TEST_TABLE + "\" WHERE measure_name = 'cpu_utilization' GROUP BY measure_name, az, hostname, region")
                .build();

        String expectedQuery = "WITH t1 AS ( select az, hostname, region,  CREATE_TIME_SERIES(time, measure_value::double) as cpu_utilization from \"my_schema\".\"my_table\" WHERE measure_name = 'cpu_utilization' GROUP BY measure_name, az, hostname, region )  SELECT region, az, hostname, cpu_utilization FROM t1 WHERE (\"az\" IN ('us-east-1a','us-east-1b'))";

        QueryResponse mockResult = makeMockQueryResult(schemaForReadView, 1_000);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals(normalizeQuery(expectedQuery), normalizeQuery(request.queryString()));
                            return mockResult;
                        }
                );

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("az", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("us-east-1a")
                .add("us-east-1b").build());

        ReadRecordsRequest request = createReadRecordsRequestWithConstraints(schemaForReadView, "default", TEST_TABLE, constraintsMap, true);
        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("readRecordsTimeSeriesView: rows[{}]", response.getRecordCount());

        for (int i = 0; i < response.getRecordCount() && i < 10; i++) {
            logger.info("readRecordsTimeSeriesView: {}", BlockUtils.rowToString(response.getRecords(), i));
        }

        logger.info("readRecordsTimeSeriesView - exit");
    }

    @Test
    public void doReadRecordsNoSpillValidateTimeStamp()
            throws Exception
    {

        int numRows = 10;
        String expectedQuery = "SELECT measure_name, measure_value::double, az, time, hostname, region FROM \"my_schema\".\"my_table\" WHERE (\"az\" IN ('us-east-1a'))";

        QueryResponse mockResult = makeMockQueryResult(schemaForRead, numRows, numRows, false);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals(normalizeQuery(expectedQuery), normalizeQuery(request.queryString()));
                            return mockResult;
                        }
                );

        Map<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("az", EquatableValueSet.newBuilder(allocator, Types.MinorType.VARCHAR.getType(), true, true)
                .add("us-east-1a").build());

        ReadRecordsRequest request = createReadRecordsRequestWithConstraints(schemaForRead, constraintsMap, 100_000_000_000L, 100_000_000_000L);
        RecordResponse rawResponse = handler.doReadRecords(allocator, request);

        assertTrue(rawResponse instanceof ReadRecordsResponse);

        ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
        logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());

        assertTrue(response.getRecords().getRowCount() > 0);

        Block block = response.getRecords();
        FieldReader time = block.getFieldReader("time");
        for (int i = 0; i < response.getRecordCount() && i < numRows; i++) {
            time.setPosition(i);
            assertTrue(time.readObject() instanceof LocalDateTime);
            assertEquals(TestUtils.startDate.plusDays(i).truncatedTo(ChronoUnit.MILLIS), time.readObject());
        }

        logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));
    }

    @Test
    public void doReadRecords_WithQueryPassthrough_ShouldReturnRecords()
            throws Exception
    {
        Map<String, String> qptArgs = new HashMap<>();
        qptArgs.put(QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, SYSTEM_QUERY_FUNCTION);
        qptArgs.put(TimestreamQueryPassthrough.QUERY, PASSTHROUGH_QUERY);

        QueryResponse mockResult = makeMockQueryResult(schemaForRead, 100);
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            QueryRequest request = (QueryRequest) invocationOnMock.getArguments()[0];
                            assertEquals(PASSTHROUGH_QUERY, request.queryString());
                            return mockResult;
                        }
                );

        ReadRecordsRequest request = createReadRecordsRequest(schemaForRead, qptArgs);
        ReadRecordsResponse response = executeReadRecords(request);
        assertTrue(response.getRecords().getRowCount() > 0);
    }

    @Test
    public void doReadRecords_WithPagination_ShouldPaginate()
            throws Exception
    {
        final int[] callCount = {0};
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            callCount[0]++;
                            QueryResponse.Builder responseBuilder = QueryResponse.builder();
                            List<Row> rows = new ArrayList<>();
                            for (int i = 0; i < 10; i++) {
                                List<Datum> columnData = new ArrayList<>();
                                // Create proper data types for each field
                                columnData.add(Datum.builder().scalarValue(MEASURE_PREFIX + i).build()); // VARCHAR
                                columnData.add(Datum.builder().scalarValue(String.valueOf(i * 1.5)).build()); // FLOAT8
                                columnData.add(Datum.builder().scalarValue(US_EAST_1A).build()); // VARCHAR
                                columnData.add(Datum.builder().scalarValue(TestUtils.startDate.plusDays(i).toString().replace('T', ' ')).build()); // DATEMILLI
                                columnData.add(Datum.builder().scalarValue(HOST_PREFIX + i).build()); // VARCHAR
                                columnData.add(Datum.builder().scalarValue(REGION_1).build()); // VARCHAR
                                rows.add(Row.builder().data(columnData).build());
                            }
                            responseBuilder.rows(rows);

                            if (callCount[0] < 3) {
                                responseBuilder.nextToken(PAGINATION_TOKEN_PREFIX + callCount[0]);
                            }
                            else {
                                responseBuilder.nextToken(null);
                            }

                            return responseBuilder.build();
                        }
                );

        ReadRecordsRequest request = createReadRecordsRequest(schemaForRead, null);
        ReadRecordsResponse response = executeReadRecords(request);
        assertTrue(response.getRecords().getRowCount() > 0);
        assertTrue(callCount[0] >= 2); // Should have paginated
    }

    @Test
    public void doReadRecords_WithNullRows_ShouldReturnEmptyResponse()
            throws Exception
    {
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            return QueryResponse.builder()
                                    .rows((List<Row>) null) // Null rows
                                    .nextToken(null)
                                    .build();
                            }
                );

        ReadRecordsRequest request = createReadRecordsRequest(schemaForRead, null);
        ReadRecordsResponse response = executeReadRecords(request);
        assertEquals(0, response.getRecords().getRowCount());
    }

    @Test
    public void doReadRecords_WithEmptyNextToken_ShouldStopPagination()
            throws Exception
    {
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            List<Row> rows = new ArrayList<>();
                            List<Datum> columnData = new ArrayList<>();
                            // Create proper data types for each field
                            columnData.add(Datum.builder().scalarValue(MEASURE_1).build()); // VARCHAR
                            columnData.add(Datum.builder().scalarValue(FLOAT_VALUE_1_5).build()); // FLOAT8
                            columnData.add(Datum.builder().scalarValue(US_EAST_1A).build()); // VARCHAR
                            columnData.add(Datum.builder().scalarValue(TestUtils.startDate.toString().replace('T', ' ')).build()); // DATEMILLI
                            columnData.add(Datum.builder().scalarValue(HOST_1).build()); // VARCHAR
                            columnData.add(Datum.builder().scalarValue(REGION_1).build()); // VARCHAR
                            rows.add(Row.builder().data(columnData).build());

                            return QueryResponse.builder()
                                    .rows(rows)
                                    .nextToken("") // Empty string should stop pagination
                                    .build();
                        }
                );

        ReadRecordsRequest request = createReadRecordsRequest(schemaForRead, null);
        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue(rawResponse instanceof ReadRecordsResponse);
    }

    @Test
    public void doReadRecords_WithBitType_ShouldReturnRecords()
            throws Exception
    {
        Schema schemaWithBit = SchemaBuilder.newBuilder()
                .addField("flag", Types.MinorType.BIT.getType())
                .build();

        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            List<Row> rows = new ArrayList<>();
                            List<Datum> columnData = new ArrayList<>();
                            columnData.add(Datum.builder().scalarValue(TRUE_VALUE).build());
                            rows.add(Row.builder().data(columnData).build());

                            return QueryResponse.builder()
                                    .rows(rows)
                                    .nextToken(null)
                                    .build();
                        }
                );

        ReadRecordsRequest request = createReadRecordsRequest(schemaWithBit, null);
        ReadRecordsResponse response = executeReadRecords(request);
        assertEquals(1, response.getRecords().getRowCount());
    }

    @Test
    public void doReadRecords_WithBigIntType_ShouldReturnRecords()
            throws Exception
    {
        Schema schemaWithBigInt = SchemaBuilder.newBuilder()
                .addField("count", Types.MinorType.BIGINT.getType())
                .build();

        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            List<Row> rows = new ArrayList<>();
                            List<Datum> columnData = new ArrayList<>();
                            columnData.add(Datum.builder().scalarValue(BIGINT_VALUE).build());
                            rows.add(Row.builder().data(columnData).build());

                            return QueryResponse.builder()
                                    .rows(rows)
                                    .nextToken(null)
                                    .build();
                        }
                );

        ReadRecordsRequest request = createReadRecordsRequest(schemaWithBigInt, null);
        ReadRecordsResponse response = executeReadRecords(request);
        assertEquals(1, response.getRecords().getRowCount());
    }

    @Test
    public void doReadRecords_WithNullValues_ShouldReturnRecords()
            throws Exception
    {
        Schema schemaWithNulls = SchemaBuilder.newBuilder()
                .addField(COLUMN_NAME_1, Types.MinorType.VARCHAR.getType())
                .addField(COLUMN_NAME_2, Types.MinorType.FLOAT8.getType())
                .addField(COLUMN_NAME_3, Types.MinorType.BIGINT.getType())
                .addField(COLUMN_NAME_4, Types.MinorType.DATEMILLI.getType())
                .build();

        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            List<Row> rows = new ArrayList<>();
                            List<Datum> columnData = new ArrayList<>();
                            columnData.add(Datum.builder().scalarValue(null).build()); // Null VARCHAR
                            columnData.add(Datum.builder().scalarValue(null).build()); // Null FLOAT8
                            columnData.add(Datum.builder().scalarValue(null).build()); // Null BIGINT
                            columnData.add(Datum.builder().scalarValue(null).build()); // Null DATEMILLI
                            rows.add(Row.builder().data(columnData).build());

                            return QueryResponse.builder()
                                    .rows(rows)
                                    .nextToken(null)
                                    .build();
                        }
                );

        ReadRecordsRequest request = createReadRecordsRequest(schemaWithNulls, null);
        ReadRecordsResponse response = executeReadRecords(request);
        assertEquals(1, response.getRecords().getRowCount());
    }

    @Test
    public void doReadRecords_WithTimeSeriesIntValue_ShouldReturnRecords()
            throws Exception
    {
        Schema schemaWithTimeSeriesInt = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("cpu_utilization", Types.MinorType.LIST.getType())
                        .addField(FieldBuilder.newBuilder("cpu_utilization", Types.MinorType.STRUCT.getType())
                                .addDateMilliField("time")
                                .addIntField("measure_value::int")
                                .build())
                        .build())
                .build();

        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            List<Row> rows = new ArrayList<>();
                            List<Datum> columnData = new ArrayList<>();

                            List<TimeSeriesDataPoint> dataPoints = new ArrayList<>();
                            for (int i = 0; i < 5; i++) {
                                TimeSeriesDataPoint dataPoint = TimeSeriesDataPoint.builder()
                                        .time(TIMESTAMP_2024_01_01)
                                        .value(Datum.builder().scalarValue(String.valueOf(i * 10)).build())
                                        .build();
                                dataPoints.add(dataPoint);
                            }

                            columnData.add(Datum.builder().timeSeriesValue(dataPoints).build());
                            rows.add(Row.builder().data(columnData).build());

                            return QueryResponse.builder()
                                    .rows(rows)
                                    .nextToken(null)
                                    .build();
                        }
                );

        ReadRecordsRequest request = createReadRecordsRequest(schemaWithTimeSeriesInt, null);
        ReadRecordsResponse response = executeReadRecords(request);
        assertEquals(1, response.getRecords().getRowCount());
    }

    @Test
    public void doReadRecords_WithTimeSeriesBitValue_ShouldReturnRecords()
            throws Exception
    {
        Schema schemaWithTimeSeriesBit = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("status", Types.MinorType.LIST.getType())
                        .addField(FieldBuilder.newBuilder("status", Types.MinorType.STRUCT.getType())
                                .addDateMilliField("time")
                                .addBitField("measure_value::bit")
                                .build())
                        .build())
                .build();

        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            List<Row> rows = new ArrayList<>();
                            List<Datum> columnData = new ArrayList<>();

                            List<TimeSeriesDataPoint> dataPoints = new ArrayList<>();
                            TimeSeriesDataPoint dataPoint = TimeSeriesDataPoint.builder()
                                    .time(TIMESTAMP_2024_01_01)
                                    .value(Datum.builder().scalarValue(TRUE_VALUE).build())
                                    .build();
                            dataPoints.add(dataPoint);

                            columnData.add(Datum.builder().timeSeriesValue(dataPoints).build());
                            rows.add(Row.builder().data(columnData).build());

                            return QueryResponse.builder()
                                    .rows(rows)
                                    .nextToken(null)
                                    .build();
                        }
                );

        ReadRecordsRequest request = createReadRecordsRequest(schemaWithTimeSeriesBit, null);
        ReadRecordsResponse response = executeReadRecords(request);
        assertEquals(1, response.getRecords().getRowCount());
    }

    @Test(expected = RuntimeException.class)
    public void doReadRecords_WithUnsupportedFieldType_ShouldThrowRuntimeException()
            throws Exception
    {
        Schema schemaWithUnsupportedType = SchemaBuilder.newBuilder()
                .addField(COLUMN_NAME_1, Types.MinorType.INT.getType()) // INT is not directly supported
                .build();

        ReadRecordsRequest request = createReadRecordsRequest(schemaWithUnsupportedType, null);
        handler.doReadRecords(allocator, request);
    }

    @Test
    public void doReadRecords_WithEmptyRowsList_ShouldReturnEmptyResponse()
            throws Exception
    {
        when(mockClient.query(nullable(QueryRequest.class)))
                .thenAnswer((Answer<QueryResponse>) invocationOnMock -> {
                            return QueryResponse.builder()
                                    .rows(new ArrayList<>()) // Empty list (not null)
                                    .nextToken(null)
                                    .build();
                        }
                );

        ReadRecordsRequest request = createReadRecordsRequest(schemaForRead, null);
        ReadRecordsResponse response = executeReadRecords(request);
        assertEquals(0, response.getRecords().getRowCount());
    }

    /**
     * Normalizes a query string by removing all whitespace characters for comparison.
     */
    private String normalizeQuery(String query)
    {
        return query.replaceAll("\\s+", " ").trim();
    }

    /**
     * Creates a ReadRecordsRequest with common default values.
     *
     * @param schema  The schema to use for the request
     * @param qptArgs Optional query passthrough arguments. If null, uses empty map.
     * @return A configured ReadRecordsRequest
     */
    private ReadRecordsRequest createReadRecordsRequest(Schema schema, Map<String, String> qptArgs)
    {
        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split split = Split.newBuilder(splitLoc, keyFactory.create()).build();

        Map<String, String> constraintsMap = qptArgs != null ? qptArgs : Collections.emptyMap();

        return new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                QUERY_ID_PREFIX + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schema,
                split,
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, constraintsMap, null),
                100_000_000_000L,
                100_000_000_000L
        );
    }

    /**
     * Creates a ReadRecordsRequest with custom constraints and spill limits.
     *
     * @param schema             The schema to use for the request
     * @param constraintsMap     The ValueSet constraints map
     * @param maxBlockSize       Maximum block size for spilling
     * @param maxInlineBlockSize Maximum inline block size
     * @return A configured ReadRecordsRequest
     */
    private ReadRecordsRequest createReadRecordsRequestWithConstraints(Schema schema, Map<String, ValueSet> constraintsMap, long maxBlockSize, long maxInlineBlockSize)
    {
        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split split = Split.newBuilder(splitLoc, keyFactory.create()).build();

        return new ReadRecordsRequest(IDENTITY,
                DEFAULT_CATALOG,
                QUERY_ID_PREFIX + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, TEST_TABLE),
                schema,
                split,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                maxBlockSize,
                maxInlineBlockSize
        );
    }

    /**
     * Creates a ReadRecordsRequest with custom catalog, table name, and constraints.
     *
     * @param schema            The schema to use for the request
     * @param catalog           The catalog name
     * @param tableName         The table name
     * @param constraintsMap    The ValueSet constraints map
     * @param useNullKeyFactory Whether to use null for keyFactory (true) or keyFactory.create() (false)
     * @return A configured ReadRecordsRequest
     */
    private ReadRecordsRequest createReadRecordsRequestWithConstraints(Schema schema, String catalog, String tableName, Map<String, ValueSet> constraintsMap, boolean useNullKeyFactory)
    {
        S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                .withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString())
                .withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true)
                .build();

        Split split = Split.newBuilder(splitLoc, useNullKeyFactory ? null : keyFactory.create()).build();

        return new ReadRecordsRequest(IDENTITY,
                catalog,
                QUERY_ID_PREFIX + System.currentTimeMillis(),
                new TableName(DEFAULT_SCHEMA, tableName),
                schema,
                split,
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
                100_000_000_000L,
                100_000_000_000L
        );
    }

    /**
     * Executes doReadRecords and returns the response, asserting it's a ReadRecordsResponse.
     *
     * @param request The ReadRecordsRequest to execute
     * @return The ReadRecordsResponse
     * @throws Exception If execution fails
     */
    private ReadRecordsResponse executeReadRecords(ReadRecordsRequest request)
            throws Exception
    {
        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue(rawResponse instanceof ReadRecordsResponse);
        return (ReadRecordsResponse) rawResponse;
    }
}
