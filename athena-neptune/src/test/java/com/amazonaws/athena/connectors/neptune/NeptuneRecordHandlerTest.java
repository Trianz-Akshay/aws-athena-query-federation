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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.neptune.propertygraph.rowwriters.CustomSchemaRowWriter;
import com.amazonaws.athena.connectors.neptune.rdf.RDFHandler;
import com.google.common.io.ByteStreams;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.atLeastOnce;

@RunWith(MockitoJUnitRunner.class)
public class NeptuneRecordHandlerTest extends TestBase
{
    private static final Logger logger = LoggerFactory.getLogger(NeptuneRecordHandlerTest.class);

    private NeptuneRecordHandler handler;
    private BlockAllocatorImpl allocator;
    private Schema schemaPGVertexForRead;
    private Schema schemaPGEdgeForRead;
    private Schema schemaPGQueryForRead;
    private S3Client amazonS3;
    private SecretsManagerClient awsSecretsManager;
    private AthenaClient athena;
    private S3BlockSpillReader spillReader;
    private EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private List<ByteHolder> mockS3Storage = new ArrayList<>();
    public CompletableFuture<List<Result>> results;

    @Mock
    private NeptuneConnection neptuneConnection;

    @Rule
    public TestName testName = new TestName();

    @After
    public void after()
    {
        allocator.close();
        logger.info("{}: exit ", testName.getMethodName());
    }

    @Before
    public void setUp()
    {
        logger.info("{}: enter", testName.getMethodName());

        schemaPGVertexForRead = SchemaBuilder
                .newBuilder()
                .addMetadata("componenttype", "vertex")
                .addStringField("id")
                .addIntField("property1")
                .addStringField("property2")
                .addFloat8Field("property3")
                .addBitField("property4")
                .addBigIntField("property5")
                .addFloat4Field("property6")
                .addDateMilliField("property7")
                .addStringField("property8")
                .build();

        schemaPGEdgeForRead = SchemaBuilder
                .newBuilder()
                .addMetadata("componenttype", "edge")
                .addStringField("in")
                .addStringField("out")
                .addStringField("id")
                .addIntField("property1")
                .addStringField("property2")
                .addFloat8Field("property3")
                .addBitField("property4")
                .addBigIntField("property5")
                .addFloat4Field("property6")
                .addDateMilliField("property7")
                .build();

        schemaPGQueryForRead = SchemaBuilder
                .newBuilder()
                .addMetadata("componenttype", "query")
                .addMetadata("query", "g.V().hasLabel('default').valueMap()")
                .addIntField("property1")
                .addStringField("property2")
                .addFloat8Field("property3")
                .addBitField("property4")
                .addBigIntField("property5")
                .addFloat4Field("property6")
                .addDateMilliField("property7")
                .build();

        allocator = new BlockAllocatorImpl();
        amazonS3 = mock(S3Client.class);
        awsSecretsManager = mock(SecretsManagerClient.class);
        athena = mock(AthenaClient.class);

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

        handler = new NeptuneRecordHandler(amazonS3, awsSecretsManager, athena, neptuneConnection, com.google.common.collect.ImmutableMap.of());
        spillReader = new S3BlockSpillReader(amazonS3, allocator);
    }

    /**
     * Create Mock Graph for testing
     */
    private void buildGraphTraversal()
    {

        GraphTraversalSource graphTraversalSource = mock(GraphTraversalSource.class);
        Client client = mock(Client.class);

        when(neptuneConnection.getNeptuneClientConnection()).thenReturn(client);
        when(neptuneConnection.getTraversalSource(nullable(Client.class))).thenReturn(graphTraversalSource);

        // Build Tinker Pop Graph
        TinkerGraph tinkerGraph = TinkerGraph.open();
        // Create new Vertex objects to add to traversal for mock
        Vertex vertex1 = tinkerGraph.addVertex(T.id, "vertex1", T.label, "default");
        vertex1.property("property1", 10);
        vertex1.property("property2", "string1");
        vertex1.property("property3", 12.4);
        vertex1.property("property4", true);
        vertex1.property("property5", 12379878123l);
        vertex1.property("property6", 15.45);
        vertex1.property("property7", (new Date()));
        vertex1.property("Property8", "string8");

        Vertex vertex2 = tinkerGraph.addVertex(T.id, "vertex2", T.label, "default");
        vertex2.property("property1", 5);
        vertex2.property("property2", "string2");
        vertex2.property("property3", 20.4);
        vertex2.property("property4", true);
        vertex2.property("property5", 12379878123l);
        vertex2.property("property6", 13.4523);
        vertex2.property("property7", (new Date()));

        Vertex vertex3 = tinkerGraph.addVertex(T.id, "vertex3", T.label, "default");
        vertex3.property("property1", 9);
        vertex3.property("property2", "string3");
        vertex3.property("property3", 15.4);
        vertex3.property("property4", true);
        vertex3.property("property5", 12379878123l);
        vertex3.property("property6", 13.4523);
        vertex3.property("property7", (new Date()));

        //add vertex with missing property values to check for nulls
        tinkerGraph.addVertex(T.label, "default");

        //add vertex to check for conversion from int to float,double.
        Vertex vertex4 = tinkerGraph.addVertex(T.label, "default");
        vertex4.property("property3", 15);
        vertex4.property("property6", 13);

        GraphTraversal<Vertex, Vertex> vertextTraversal = (GraphTraversal<Vertex, Vertex>) tinkerGraph.traversal().V();
        when(graphTraversalSource.V()).thenReturn(vertextTraversal);

        //add edge from vertex1 to vertex2
        tinkerGraph.traversal().addE("default").from(vertex1).to(vertex2).property(T.id, "vertex1-vertex2").next();

        //add edge from vertex1 to vertex2 with attributes
        tinkerGraph.traversal().addE("default").from(vertex2).to(vertex3).property(T.id, "vertex2-vertex3").property(Cardinality.single, "property1", 21).next();

        GraphTraversal<Edge, Edge> edgeTraversal = (GraphTraversal<Edge, Edge>) tinkerGraph.traversal().E();
    }

    @Test
    public void doReadRecordsSpill() throws Exception
    {
        S3SpillLocation splitLoc = S3SpillLocation.newBuilder().withBucket(UUID.randomUUID().toString())
                .withSplitId(UUID.randomUUID().toString()).withQueryId(UUID.randomUUID().toString())
                .withIsDirectory(true).build();

        allocator = new BlockAllocatorImpl();

        // Greater Than filter
        HashMap<String, ValueSet> constraintsMap = new HashMap<>();
        constraintsMap.put("property1",
                SortedRangeSet.of(Range.greaterThan(allocator, Types.MinorType.INT.getType(), 9)));

        buildGraphTraversal();

        ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY, DEFAULT_CATALOG, QUERY_ID, TABLE_NAME,
                schemaPGVertexForRead, Split.newBuilder(splitLoc, keyFactory.create()).build(),
                new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()), 1_500_000L, // ~1.5MB so we should see some spill
                0L);

        RecordResponse rawResponse = handler.doReadRecords(allocator, request);
        assertTrue(rawResponse instanceof RemoteReadRecordsResponse);

        try (RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse) {
            logger.info("doReadRecordsSpill: remoteBlocks[{}]", response.getRemoteBlocks().size());

            assertTrue(response.getNumberBlocks() == 1);

            int blockNum = 0;
            for (SpillLocation next : response.getRemoteBlocks()) {
                S3SpillLocation spillLocation = (S3SpillLocation) next;
                try (Block block = spillReader.read(spillLocation, response.getEncryptionKey(),
                        response.getSchema())) {
                    logger.info("doReadRecordsSpill: blockNum[{}] and recordCount[{}]", blockNum++,
                            block.getRowCount());

                    logger.info("doReadRecordsSpill: {}", BlockUtils.rowToString(block, 0));
                    assertNotNull(BlockUtils.rowToString(block, 0));
                }
            }
        }
    }

    @Test
    public void testReadWithConstraint_PropertyGraph_VertexType() throws Exception
    {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("neptune.graph.type", "propertygraph");

        // Mock schema fields
        List<Field> fields = Arrays.asList(
                FieldBuilder.newBuilder("id", ArrowType.Utf8.INSTANCE).build(),
                FieldBuilder.newBuilder("name", ArrowType.Utf8.INSTANCE).build()
        );

        Map<String, String> customMeta = new HashMap<>();
        customMeta.put(Constants.SCHEMA_COMPONENT_TYPE, "VERTEX");
        customMeta.put(Constants.SCHEMA_GLABEL, "person");

        Schema schema = new Schema(fields, customMeta);

        // Build ReadRecordsRequest
        ReadRecordsRequest request = new ReadRecordsRequest(
                IDENTITY,
                DEFAULT_CATALOG,
                QUERY_ID,
                TABLE_NAME,
                schema,
                Split.newBuilder(S3SpillLocation.newBuilder()
                        .withBucket("bucket")
                        .withSplitId("split")
                        .withQueryId("query")
                        .withIsDirectory(true).build(), keyFactory.create()).build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()),
                1_500_000L,
                0L
        );

        // Mocks
        Client mockClient = mock(Client.class);
        GraphTraversalSource mockGts = mock(GraphTraversalSource.class);
        GraphTraversal mockTraversal = mock(GraphTraversal.class);

        // Set up NeptuneConnection mocks
        when(neptuneConnection.getNeptuneClientConnection()).thenReturn(mockClient);
        when(neptuneConnection.getTraversalSource(mockClient)).thenReturn(mockGts);

        // Setup Gremlin traversal mocks
        when(mockGts.V()).thenReturn(mockTraversal);
        when(mockTraversal.hasLabel("person")).thenReturn(mockTraversal);
        when(mockTraversal.valueMap()).thenReturn(mockTraversal);
        when(mockTraversal.with(WithOptions.tokens)).thenReturn(mockTraversal);

        BlockSpiller mockSpiller = mock(BlockSpiller.class);
        QueryStatusChecker mockChecker = mock(QueryStatusChecker.class);

        // Real handler under test
        NeptuneRecordHandler handler = new NeptuneRecordHandler(amazonS3, awsSecretsManager, athena, neptuneConnection, configOptions);

        // Run method
        handler.readWithConstraint(mockSpiller, request, mockChecker);

        // Verify traversal was used
        verify(mockGts).V();
        verify(mockTraversal).hasLabel("person");
        verify(mockTraversal).valueMap();
        verify(mockTraversal).with(WithOptions.tokens);
    }

    @Test
    public void testReadWithConstraint_PropertyGraph_EdgeType() throws Exception
    {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("neptune.graph.type", "propertygraph");

        List<Field> fields = Arrays.asList(
                FieldBuilder.newBuilder("id", ArrowType.Utf8.INSTANCE).build(),
                FieldBuilder.newBuilder("weight", new ArrowType.Int(32, true)).build()
        );

        Map<String, String> customMeta = new HashMap<>();
        customMeta.put(Constants.SCHEMA_COMPONENT_TYPE, "EDGE");
        customMeta.put(Constants.SCHEMA_GLABEL, "knows");

        Schema schema = new Schema(fields, customMeta);

        ReadRecordsRequest request = new ReadRecordsRequest(
                IDENTITY,
                DEFAULT_CATALOG,
                QUERY_ID,
                TABLE_NAME,
                schema,
                Split.newBuilder(S3SpillLocation.newBuilder()
                        .withBucket("bucket")
                        .withSplitId("split")
                        .withQueryId("query")
                        .withIsDirectory(true).build(), keyFactory.create()).build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()),
                1_500_000L,
                0L
        );

        Client mockClient = mock(Client.class);
        GraphTraversalSource mockGts = mock(GraphTraversalSource.class);
        GraphTraversal mockTraversal = mock(GraphTraversal.class);

        when(neptuneConnection.getNeptuneClientConnection()).thenReturn(mockClient);
        when(neptuneConnection.getTraversalSource(mockClient)).thenReturn(mockGts);

        when(mockGts.E()).thenReturn(mockTraversal);
        when(mockTraversal.hasLabel("knows")).thenReturn(mockTraversal);
        when(mockTraversal.elementMap()).thenReturn(mockTraversal);

        BlockSpiller mockSpiller = mock(BlockSpiller.class);
        QueryStatusChecker mockChecker = mock(QueryStatusChecker.class);

        NeptuneRecordHandler handler = new NeptuneRecordHandler(amazonS3, awsSecretsManager, athena, neptuneConnection, configOptions);

        handler.readWithConstraint(mockSpiller, request, mockChecker);

        verify(mockGts).E();
        verify(mockTraversal).hasLabel("knows");
        verify(mockTraversal).elementMap();
    }

    @Test
    public void testReadWithConstraint_PropertyGraph_ViewType() throws Exception
    {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("neptune.graph.type", "propertygraph");

        List<Field> fields = Arrays.asList(
                FieldBuilder.newBuilder("id", ArrowType.Utf8.INSTANCE).build(),
                FieldBuilder.newBuilder("age", new ArrowType.Int(32, true)).build()
        );

        Map<String, String> customMeta = new HashMap<>();
        customMeta.put(Constants.SCHEMA_COMPONENT_TYPE, "VIEW");
        customMeta.put(Constants.SCHEMA_QUERY, "g.V().hasLabel('person').valueMap()");

        Schema schema = new Schema(fields, customMeta);

        ReadRecordsRequest request = new ReadRecordsRequest(
                IDENTITY,
                DEFAULT_CATALOG,
                QUERY_ID,
                TABLE_NAME,
                schema,
                Split.newBuilder(S3SpillLocation.newBuilder()
                        .withBucket("bucket")
                        .withSplitId("split")
                        .withQueryId("query")
                        .withIsDirectory(true).build(), keyFactory.create()).build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()),
                1_500_000L,
                0L
        );

        Client mockClient = mock(Client.class);
        Result mockResult = mock(Result.class);
        Iterator<Result> mockIterator = mock(Iterator.class);
        ResultSet mockResultSet = mock(ResultSet.class);

        when(neptuneConnection.getNeptuneClientConnection()).thenReturn(mockClient);
        when(mockClient.submit("g.V().hasLabel('person').valueMap()")).thenReturn(mockResultSet);
        when(mockResultSet.iterator()).thenReturn(mockIterator);
        when(mockIterator.hasNext()).thenReturn(true, false);

        // Proper type to avoid ClassCastException
        Map<String, List<Object>> gremlinResult = new HashMap<>();
        gremlinResult.put("id", Collections.singletonList("1"));
        gremlinResult.put("age", Collections.singletonList(42));
        when(mockResult.getObject()).thenReturn(gremlinResult);
        when(mockIterator.next()).thenReturn(mockResult);

        BlockSpiller mockSpiller = mock(BlockSpiller.class);
        QueryStatusChecker mockChecker = mock(QueryStatusChecker.class);
        when(mockChecker.isQueryRunning()).thenReturn(true);

        // Mock static for GeneratedRowWriter
        try (MockedStatic<GeneratedRowWriter> mockedWriter = mockStatic(GeneratedRowWriter.class)) {
            GeneratedRowWriter.RowWriterBuilder builderMock = mock(GeneratedRowWriter.RowWriterBuilder.class);
            GeneratedRowWriter rowWriterMock = mock(GeneratedRowWriter.class);

            mockedWriter.when(() -> GeneratedRowWriter.newBuilder(any())).thenReturn(builderMock);
            when(builderMock.build()).thenReturn(rowWriterMock);
            when(rowWriterMock.writeRow(any(), anyInt(), any())).thenReturn(true);

            // Optional: mock field templates
            mockStatic(CustomSchemaRowWriter.class).when(() ->
                    CustomSchemaRowWriter.writeRowTemplate(any(), any(), anyMap())
            ).thenAnswer(inv -> null); // no-op

            // simulate spilling block
            doAnswer(invocation -> {
                BlockWriter.RowWriter consumer = invocation.getArgument(0);
                Block mockBlock = mock(Block.class);
                consumer.writeRows(mockBlock, 0);
                return null;
            }).when(mockSpiller).writeRows(any());

            NeptuneRecordHandler handler = new NeptuneRecordHandler(
                    amazonS3, awsSecretsManager, athena, neptuneConnection, configOptions);

            handler.readWithConstraint(mockSpiller, request, mockChecker);

            verify(mockClient).submit("g.V().hasLabel('person').valueMap()");
            verify(mockIterator, atLeastOnce()).hasNext();
        }
    }

    @Test
    public void testReadWithConstraint_RDF() throws Exception
    {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("neptune_graphtype", "rdf");
        // Build custom metadata for RDF queryMode = class
        Map<String, String> customMetadata = new HashMap<>();
        customMetadata.put("schema.querymode", "class");
        customMetadata.put("schema.classuri", "<http://example.org/Class>");
        customMetadata.put("schema.predsPrefix", "ex");
        customMetadata.put("schema.subject", "subject");

        List<Field> fields = Arrays.asList(
                new Field("subject", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
        );
        Schema schema = new Schema(fields, customMetadata);

        ReadRecordsRequest request = new ReadRecordsRequest(
                IDENTITY,
                DEFAULT_CATALOG,
                QUERY_ID,
                TABLE_NAME,
                schema,
                Split.newBuilder(S3SpillLocation.newBuilder()
                        .withBucket("bucket")
                        .withSplitId("split")
                        .withQueryId("query")
                        .withIsDirectory(true).build(), keyFactory.create()).build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()),
                1_500_000L,
                0L
        );
        BlockSpiller mockSpiller = mock(BlockSpiller.class);
        QueryStatusChecker mockChecker = mock(QueryStatusChecker.class);

        try (MockedConstruction<RDFHandler> mocked = mockConstruction(
                RDFHandler.class,
                (mock, context) -> {
                    doNothing().when(mock).executeQuery(eq(request), any(), any(), any());
                }
        )) {
            NeptuneRecordHandler handler = new NeptuneRecordHandler(
                    amazonS3, awsSecretsManager, athena, neptuneConnection, configOptions);

            handler.readWithConstraint(mockSpiller, request, mockChecker);

            // Verify RDFHandler was constructed and called
            RDFHandler constructedHandler = mocked.constructed().get(0);
            verify(constructedHandler, times(1)).executeQuery(eq(request), eq(mockChecker), eq(mockSpiller), eq(configOptions));
        }
    }

    @Test(expected = RuntimeException.class)
    public void testReadWithConstraint_RDF_ClassCastException() throws Exception
    {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("neptune_graphtype", "rdf");

        Schema schema = createRDFSchema();
        ReadRecordsRequest request = createRequest(schema);

        BlockSpiller mockSpiller = mock(BlockSpiller.class);
        QueryStatusChecker mockChecker = mock(QueryStatusChecker.class);

        try (MockedConstruction<RDFHandler> mocked = mockConstruction(
                RDFHandler.class,
                (mock, context) -> {
                    doThrow(new ClassCastException("Test CCE")).when(mock).executeQuery(any(), any(), any(), any());
                }
        )) {
            NeptuneRecordHandler handler = new NeptuneRecordHandler(
                    amazonS3, awsSecretsManager, athena, neptuneConnection, configOptions);

            handler.readWithConstraint(mockSpiller, request, mockChecker);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testReadWithConstraint_RDF_IllegalArgumentException() throws Exception
    {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("neptune_graphtype", "rdf");

        Schema schema = createRDFSchema();
        ReadRecordsRequest request = createRequest(schema);

        BlockSpiller mockSpiller = mock(BlockSpiller.class);
        QueryStatusChecker mockChecker = mock(QueryStatusChecker.class);

        try (MockedConstruction<RDFHandler> mocked = mockConstruction(
                RDFHandler.class,
                (mock, context) -> {
                    doThrow(new IllegalArgumentException("Test IAE")).when(mock).executeQuery(any(), any(), any(), any());
                }
        )) {
            NeptuneRecordHandler handler = new NeptuneRecordHandler(
                    amazonS3, awsSecretsManager, athena, neptuneConnection, configOptions);

            handler.readWithConstraint(mockSpiller, request, mockChecker);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testReadWithConstraint_RDF_RuntimeException() throws Exception
    {
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put("neptune_graphtype", "rdf");

        Schema schema = createRDFSchema();
        ReadRecordsRequest request = createRequest(schema);

        BlockSpiller mockSpiller = mock(BlockSpiller.class);
        QueryStatusChecker mockChecker = mock(QueryStatusChecker.class);

        try (MockedConstruction<RDFHandler> mocked = mockConstruction(
                RDFHandler.class,
                (mock, context) -> {
                    doThrow(new RuntimeException("Test RTE")).when(mock).executeQuery(any(), any(), any(), any());
                }
        )) {
            NeptuneRecordHandler handler = new NeptuneRecordHandler(
                    amazonS3, awsSecretsManager, athena, neptuneConnection, configOptions);

            handler.readWithConstraint(mockSpiller, request, mockChecker);
        }
    }

    private Schema createRDFSchema()
    {
        Map<String, String> customMetadata = new HashMap<>();
        customMetadata.put("schema.querymode", "class");
        customMetadata.put("schema.classuri", "<http://example.org/Class>");
        customMetadata.put("schema.predsPrefix", "ex");
        customMetadata.put("schema.subject", "subject");

        List<Field> fields = Arrays.asList(
                new Field("subject", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
        );
        return new Schema(fields, customMetadata);
    }

    private ReadRecordsRequest createRequest(Schema schema)
    {
        return new ReadRecordsRequest(
                IDENTITY,
                DEFAULT_CATALOG,
                QUERY_ID,
                TABLE_NAME,
                schema,
                Split.newBuilder(S3SpillLocation.newBuilder()
                        .withBucket("bucket")
                        .withSplitId("split")
                        .withQueryId("query")
                        .withIsDirectory(true).build(), keyFactory.create()).build(),
                new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()),
                1_500_000L,
                0L
        );
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
