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
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.*;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class NeptuneRecordHandlerTest extends TestBase {
    private static final FederatedIdentity IDENTITY = new FederatedIdentity("testPrincipal", "testAccount", Collections.emptyMap(), Collections.emptyList());
    private static final String DEFAULT_CATALOG = "default";
    private static final String QUERY_ID = "query_id";
    private static final TableName TABLE_NAME = new TableName("default", "table");

    @Mock
    private NeptuneConnection neptuneConnection;
    @Mock
    private GraphTraversalSource mockTraversalSource;
    @Mock
    private GraphTraversal<Vertex, Vertex> mockVertexTraversal;
    @Mock
    private GraphTraversal<Edge, Edge> mockEdgeTraversal;
    @Mock
    private GraphTraversal<?, Map<Object, Object>> mockValueMapTraversal;
    @Mock
    private Vertex mockVertex;
    @Mock
    private BlockSpiller spiller;
    @Mock
    private QueryStatusChecker checker;

    private NeptuneRecordHandler handler;
    private BlockAllocatorImpl allocator;
    private S3Client amazonS3;
    private SecretsManagerClient awsSecretsManager;
    private AthenaClient athena;
    private S3BlockSpillReader spillReader;
    private final List<ByteHolder> mockS3Storage = new ArrayList<>();
    private Map<String, String> configOptions;

    @Before
    public void setUp() {
        allocator = new BlockAllocatorImpl();
        amazonS3 = mock(S3Client.class);
        awsSecretsManager = mock(SecretsManagerClient.class);
        athena = mock(AthenaClient.class);
        configOptions = new HashMap<>();
        configOptions.put(Constants.CFG_ENDPOINT, "localhost");
        configOptions.put(Constants.CFG_PORT, "8182");
        configOptions.put(Constants.CFG_GRAPH_TYPE, "PROPERTYGRAPH");
        configOptions.put(Constants.CFG_IAM, "false");
        configOptions.put(Constants.CFG_REGION, "us-east-1");
        setupS3Mocks();
        //Client mockClient = mock(Client.class);
        //when(neptuneConnection.getNeptuneClientConnection()).thenReturn(mockClient);
        handler = new NeptuneRecordHandler(amazonS3, awsSecretsManager, athena, neptuneConnection, configOptions);
        //when(checker.isQueryRunning()).thenReturn(true);
    }

    @After
    public void after() {
        allocator.close();
    }

    private void setupS3Mocks() {
//        when(amazonS3.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
//            .thenAnswer(inv -> {
//                RequestBody requestBody = inv.getArgument(1);
//                InputStream inputStream = requestBody.contentStreamProvider().newStream();
//                ByteHolder holder = new ByteHolder();
//                holder.setBytes(com.google.common.io.ByteStreams.toByteArray(inputStream));
//                mockS3Storage.add(holder);
//                return PutObjectResponse.builder().build();
//            });
//
//        when(amazonS3.getObject(any(GetObjectRequest.class)))
//            .thenAnswer(inv -> {
//                ByteHolder holder = mockS3Storage.remove(0);
//                return new ResponseInputStream<>(
//                    GetObjectResponse.builder().build(),
//                    new ByteArrayInputStream(holder.getBytes())
//                );
//            });
    }

    private void setupGraphTraversalMocks() {
        when(mockTraversalSource.V()).thenReturn(mockVertexTraversal);
        when(mockTraversalSource.E()).thenReturn(mockEdgeTraversal);
        when(mockVertexTraversal.valueMap()).thenReturn((GraphTraversal) mockValueMapTraversal);
        when(mockEdgeTraversal.valueMap()).thenReturn((GraphTraversal) mockValueMapTraversal);
        when(mockValueMapTraversal.hasNext()).thenReturn(true, false);
        when(mockValueMapTraversal.next()).thenReturn(Collections.singletonMap("id", Collections.singletonList("1")));
        when(mockVertex.id()).thenReturn("1");
        when(mockVertex.label()).thenReturn("person");
        when(mockVertex.values("name")).thenReturn((Iterator) Collections.singletonList("John").iterator());
        when(mockVertex.values("age")).thenReturn((Iterator) Collections.singletonList(30).iterator());
    }

    /*
    @Test
    public void testPropertyGraphVertexRead() throws Exception {
        setupGraphTraversalMocks();
        Schema schema = SchemaBuilder.newBuilder()
            .addStringField("id")
            .addStringField("label")
            .addStringField("name")
            .addIntField("age")
            .build();

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        handler.readWithConstraint(spiller, request, checker);

        verify(mockTraversalSource).V();
        verify(spiller).writeRows(any());
    }
    */

    /*
    @Test
    public void testPropertyGraphEdgeRead() throws Exception {
        setupGraphTraversalMocks();
        Schema schema = SchemaBuilder.newBuilder()
            .addStringField("id")
            .addStringField("label")
            .addStringField("name")
            .addIntField("age")
            .build();

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        handler.readWithConstraint(spiller, request, checker);

        verify(mockTraversalSource).E();
        verify(spiller).writeRows(any());
    }
    */

    /*
    @Test
    public void testPropertyGraphQueryPassthrough() throws Exception {
        setupGraphTraversalMocks();
        Schema schema = SchemaBuilder.newBuilder()
            .addStringField("id")
            .addStringField("label")
            .addStringField("name")
            .addIntField("age")
            .build();

        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put("SYSTEM.QUERY", "g.V().hasLabel('person')");
        passthroughArgs.put("database", "default");
        passthroughArgs.put("collection", "vertices");

        ReadRecordsRequest request = createReadRecordsRequestWithPassthrough(schema, passthroughArgs);
        handler.readWithConstraint(spiller, request, checker);

        verify(mockTraversalSource).V();
        verify(spiller).writeRows(any());
    }
    */

    @Test
    public void testRDFRead() throws Exception {
        configOptions.put(Constants.CFG_GRAPH_TYPE, "RDF");

        Schema schema = createRDFSchema();
        ReadRecordsRequest request = createReadRecordsRequest(schema);

        try (MockedConstruction<RDFHandler> mocked = mockConstruction(RDFHandler.class)) {
            handler = new NeptuneRecordHandler(amazonS3, awsSecretsManager, athena, neptuneConnection, configOptions);
            handler.readWithConstraint(spiller, request, checker);
            verify(mocked.constructed().get(0)).executeQuery(eq(request), any(), any(), any());
        }
    }

    @Test(expected = RuntimeException.class)
    public void testRDFReadErrors() throws Exception {
        configOptions.put(Constants.CFG_GRAPH_TYPE, "RDF");

        Schema schema = createRDFSchema();
        ReadRecordsRequest request = createReadRecordsRequest(schema);

        try (MockedConstruction<RDFHandler> mocked = mockConstruction(
                RDFHandler.class,
                (mock, ctx) -> doThrow(new RuntimeException()).when(mock).executeQuery(any(), any(), any(), any()))) {
            handler = new NeptuneRecordHandler(amazonS3, awsSecretsManager, athena, neptuneConnection, configOptions);
            handler.readWithConstraint(spiller, request, checker);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidGraphType() throws Exception {
        configOptions.put(Constants.CFG_GRAPH_TYPE, "INVALID_TYPE");
        
        handler = new NeptuneRecordHandler(amazonS3, awsSecretsManager, athena, neptuneConnection, configOptions);
        
        Schema schema = SchemaBuilder.newBuilder()
            .addMetadata("componenttype", "vertex")
            .addStringField("id")
            .build();

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        handler.readWithConstraint(spiller, request, checker);
    }

    /*
    @Test
    public void testPropertyGraphQueryPassthroughInvalidQuery() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
            .addMetadata("componenttype", "vertex")
            .addStringField("id")
            .build();

        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put("system.query", "INVALID_QUERY");
        passthroughArgs.put("database", "default");
        passthroughArgs.put("collection", "person");

        ReadRecordsRequest request = createReadRecordsRequestWithPassthrough(schema, passthroughArgs);
        Client client = mock(Client.class);
        when(neptuneConnection.getNeptuneClientConnection()).thenReturn(client);
        when(neptuneConnection.getTraversalSource(any())).thenThrow(new RuntimeException("Invalid query"));

        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker checker = mock(QueryStatusChecker.class);
        when(checker.isQueryRunning()).thenReturn(true);

        handler.readWithConstraint(spiller, request, checker);
    }
    */

    @Test
    public void testRDFQueryPassthrough() throws Exception {
        configOptions.put(Constants.CFG_GRAPH_TYPE, "RDF");

        Schema schema = createRDFSchema();
        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put("system.query", "SELECT ?s ?p ?o WHERE { ?s ?p ?o }");
        passthroughArgs.put("database", "default");
        passthroughArgs.put("collection", "triples");

        ReadRecordsRequest request = createReadRecordsRequestWithPassthrough(schema, passthroughArgs);
        
        BlockSpiller spiller = mock(BlockSpiller.class);
        QueryStatusChecker checker = mock(QueryStatusChecker.class);
        //when(checker.isQueryRunning()).thenReturn(true);

        try (MockedConstruction<RDFHandler> mocked = mockConstruction(RDFHandler.class)) {
            handler = new NeptuneRecordHandler(amazonS3, awsSecretsManager, athena, neptuneConnection, configOptions);
            handler.readWithConstraint(spiller, request, checker);
            verify(mocked.constructed().get(0)).executeQuery(eq(request), any(), any(), any());
        }
    }

    private Schema createRDFSchema() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("schema.querymode", "class");
        metadata.put("schema.classuri", "<http://example.org/Class>");
        metadata.put("schema.predsPrefix", "ex");
        metadata.put("schema.subject", "subject");

        List<Field> fields = Arrays.asList(
            new Field("subject", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("name", FieldType.nullable(new ArrowType.Utf8()), null)
        );
        return new Schema(fields, metadata);
    }

    private ReadRecordsRequest createReadRecordsRequest(Schema schema) {
        S3SpillLocation spillLoc = S3SpillLocation.newBuilder()
            .withBucket(UUID.randomUUID().toString())
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(UUID.randomUUID().toString())
            .withIsDirectory(true)
            .build();

        return new ReadRecordsRequest(
            IDENTITY,
            DEFAULT_CATALOG,
            QUERY_ID,
            TABLE_NAME,
            schema,
            Split.newBuilder(spillLoc, new LocalKeyFactory().create()).build(),
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap()),
            1_500_000L,
            0L
        );
    }

    private ReadRecordsRequest createReadRecordsRequestWithPassthrough(Schema schema, Map<String, String> passthroughArgs) {
        S3SpillLocation spillLoc = S3SpillLocation.newBuilder()
            .withBucket(UUID.randomUUID().toString())
            .withSplitId(UUID.randomUUID().toString())
            .withQueryId(UUID.randomUUID().toString())
            .withIsDirectory(true)
            .build();

        return new ReadRecordsRequest(
            IDENTITY,
            DEFAULT_CATALOG,
            QUERY_ID,
            TABLE_NAME,
            schema,
            Split.newBuilder(spillLoc, new LocalKeyFactory().create()).build(),
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, passthroughArgs),
            1_500_000L,
            0L
        );
    }

    private static class ByteHolder {
        private byte[] bytes;
        public void setBytes(byte[] bytes) { this.bytes = bytes; }
        public byte[] getBytes() { return bytes; }
    }
}
