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
package com.amazonaws.athena.connectors.neptune.rdf;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.neptune.Constants;
import com.amazonaws.athena.connectors.neptune.TestBase;
import com.amazonaws.athena.connectors.neptune.qpt.NeptuneSparqlQueryPassthrough;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RDFHandlerTest extends TestBase {
    private RDFHandler handler;
    @Mock
    private NeptuneSparqlConnection sparqlConnection;
    @Mock
    private BlockSpiller spiller;
    @Mock
    private QueryStatusChecker checker;

    @Before
    public void setUp() throws Exception {
        doNothing().when(sparqlConnection).runQuery(anyString());
        handler = new RDFHandler(sparqlConnection);
        checker = mock(QueryStatusChecker.class);
        when(checker.isQueryRunning()).thenReturn(true);
        spiller = mock(BlockSpiller.class);
        doAnswer(invocation -> null).when(spiller).writeRows(any());
    }

    @Test
    public void testExecuteQueryWithSparqlMode() throws Exception {
        Schema schema = createRDFSchema();
        ReadRecordsRequest request = createReadRecordsRequest(schema);
        when(sparqlConnection.hasNext()).thenReturn(true, false);
        Map<String, Object> result = createTestResult();
        when(sparqlConnection.next()).thenReturn(result);

        handler.executeQuery(request, checker, spiller, Collections.emptyMap());

        verify(sparqlConnection).runQuery(argThat(query ->
                 query.contains("SELECT ?s ?p ?o WHERE")));
        verify(spiller, times(1)).writeRows(any());
    }

    @Test(expected = RuntimeException.class)
    public void testRDFReadErrors() throws Exception {
        Schema schema = createRDFSchema();
        ReadRecordsRequest request = createReadRecordsRequest(schema);
        doThrow(new RuntimeException()).when(sparqlConnection).runQuery(anyString());

        handler.executeQuery(request, checker, spiller, Collections.emptyMap());
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidGraphType() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
            .addMetadata("componenttype", "vertex")
            .addStringField("id")
            .build();

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        handler.executeQuery(request, checker, spiller, Collections.emptyMap());
    }

    @Test
    public void testExecuteQueryWithClassMode() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
                .addMetadata(Constants.SCHEMA_QUERY_MODE, Constants.QUERY_MODE_CLASS)
                .addMetadata(Constants.SCHEMA_CLASS_URI, "<http://example.org/Person>")
                .addMetadata(Constants.SCHEMA_PREDS_PREFIX, "ex")
                .addMetadata(Constants.SCHEMA_SUBJECT, "person")
                .addStringField("person")
                .addStringField("name")
                .addStringField("age")
                .build();

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        when(sparqlConnection.hasNext()).thenReturn(true, false);
        Map<String, Object> result = new HashMap<>();
        result.put("person", "http://example.org/person1");
        result.put("name", "John Doe");
        result.put("age", "30");
        when(sparqlConnection.next()).thenReturn(result);
        when(checker.isQueryRunning()).thenReturn(true);

        handler.executeQuery(request, checker, spiller, Collections.emptyMap());

        verify(sparqlConnection).runQuery(anyString());
        verify(spiller, times(1)).writeRows(any());
    }

    @Test(expected = RuntimeException.class)
    public void testExecuteQueryWithInvalidQueryMode() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
                .addMetadata(Constants.SCHEMA_QUERY_MODE, "invalid")
                .addMetadata(Constants.SCHEMA_COMPONENT_TYPE, "invalid")
                .addStringField("s")
                .build();

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        handler.executeQuery(request, checker, spiller, Collections.emptyMap());
    }

    @Test(expected = RuntimeException.class)
    public void testExecuteQueryWithMissingClassUri() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
            .addMetadata(Constants.SCHEMA_QUERY_MODE, "class")
            .addMetadata(Constants.SCHEMA_PREDS_PREFIX, "ex")
            .addMetadata(Constants.SCHEMA_SUBJECT, "person")
            .addMetadata(Constants.SCHEMA_COMPONENT_TYPE, "class")
            .addStringField("person")
            .build();

        ReadRecordsRequest request = createReadRecordsRequest(schema);
        handler.executeQuery(request, checker, spiller, Collections.emptyMap());
    }

    @Test
    public void testRDFQueryPassthrough() throws Exception {
        Schema schema = SchemaBuilder.newBuilder()
                .addMetadata(Constants.SCHEMA_QUERY_MODE, Constants.QUERY_MODE_SPARQL)
                .addMetadata(Constants.SCHEMA_COMPONENT_TYPE, "sparql")
                .addMetadata("schema.function", "system.query")
                .addStringField("s")
                .addStringField("p")
                .addStringField("o")
                .build();

        Map<String, String> passthroughArgs = new HashMap<>();
        passthroughArgs.put("schemaFunctionName", "SYSTEM.QUERY");
        passthroughArgs.put(NeptuneSparqlQueryPassthrough.DATABASE, "default");
        passthroughArgs.put(NeptuneSparqlQueryPassthrough.COLLECTION, "triples");
        passthroughArgs.put(NeptuneSparqlQueryPassthrough.QUERY, "SELECT ?s ?p ?o WHERE { ?s ?p ?o }");

        ReadRecordsRequest request = createReadRecordsRequestWithPassthrough(schema, passthroughArgs);
        when(sparqlConnection.hasNext()).thenReturn(true, false);
        Map<String, Object> result = createTestResult();
        when(sparqlConnection.next()).thenReturn(result);

        handler.executeQuery(request, checker, spiller, Collections.emptyMap());

        verify(sparqlConnection).runQuery(anyString());
        verify(spiller, times(1)).writeRows(any());
    }

    private Map<String, Object> createTestResult() {
        Map<String, Object> result = new HashMap<>();
        result.put("s", "http://example.org/subject");
        result.put("p", "http://example.org/predicate");
        result.put("o", "http://example.org/object");
        return result;
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
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null),
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
            new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT, passthroughArgs, null),
            1_500_000L,
            0L
        );
    }

    private Schema createRDFSchema() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(Constants.SCHEMA_QUERY_MODE, Constants.QUERY_MODE_SPARQL);
        metadata.put(Constants.SCHEMA_COMPONENT_TYPE, "sparql");
        metadata.put(Constants.PREFIX_KEY + "ex", "http://example.org/");
        metadata.put("sparql", "SELECT ?s ?p ?o WHERE { ?s ?p ?o }");

        List<Field> fields = Arrays.asList(
            new Field("s", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("p", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("o", FieldType.nullable(new ArrowType.Utf8()), null)
        );
        return new Schema(fields, metadata);
    }
} 