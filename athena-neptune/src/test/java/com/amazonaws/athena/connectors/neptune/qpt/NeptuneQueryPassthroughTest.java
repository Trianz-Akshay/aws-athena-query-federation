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
package com.amazonaws.athena.connectors.neptune.qpt;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class NeptuneQueryPassthroughTest {
    private NeptuneQueryPassthrough queryPassthrough;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        queryPassthrough = new NeptuneQueryPassthrough();
    }

    @Test
    public void getFunctionSchema_WhenCalled_ReturnsSystemSchema() {
        assertEquals("system", queryPassthrough.getFunctionSchema());
    }

    @Test
    public void getFunctionName_WhenCalled_ReturnsQueryName() {
        assertEquals("query", queryPassthrough.getFunctionName());
    }

    @Test
    public void getFunctionArguments_WhenCalled_ReturnsExpectedArgumentsList() {
        List<String> expectedArgs = Arrays.asList("DATABASE", "COLLECTION", "QUERY");
        assertEquals(expectedArgs, queryPassthrough.getFunctionArguments());
    }

    @Test
    public void verify_WithValidArguments_DoesNotThrowException() {
        Map<String, String> args = new HashMap<>();
        args.put("DATABASE", "testDb");
        args.put("COLLECTION", "testCollection");
        args.put("QUERY", "g.V().hasLabel('person')");
        args.put("schemaFunctionName", "SYSTEM.QUERY");

        // Should not throw any exception
        queryPassthrough.verify(args);
    }

    @Test(expected = AthenaConnectorException.class)
    public void verify_WithMissingDatabase_ThrowsAthenaConnectorException() {
        Map<String, String> args = new HashMap<>();
        args.put("COLLECTION", "testCollection");
        args.put("QUERY", "g.V().hasLabel('person')");

        queryPassthrough.verify(args);
    }

    @Test(expected = AthenaConnectorException.class)
    public void verify_WithMissingCollection_ThrowsAthenaConnectorException() {
        Map<String, String> args = new HashMap<>();
        args.put("DATABASE", "testDb");
        args.put("QUERY", "g.V().hasLabel('person')");

        queryPassthrough.verify(args);
    }

    @Test(expected = AthenaConnectorException.class)
    public void verify_WithMissingQuery_ThrowsAthenaConnectorException() {
        Map<String, String> args = new HashMap<>();
        args.put("DATABASE", "testDb");
        args.put("COLLECTION", "testCollection");

        queryPassthrough.verify(args);
    }

    @Test(expected = AthenaConnectorException.class)
    public void verify_WithEmptyDatabase_ThrowsAthenaConnectorException() {
        Map<String, String> args = new HashMap<>();
        args.put("DATABASE", "");
        args.put("COLLECTION", "testCollection");
        args.put("QUERY", "g.V().hasLabel('person')");

        queryPassthrough.verify(args);
    }

    @Test(expected = AthenaConnectorException.class)
    public void verify_WithEmptyCollection_ThrowsAthenaConnectorException() {
        Map<String, String> args = new HashMap<>();
        args.put("DATABASE", "testDb");
        args.put("COLLECTION", "");
        args.put("QUERY", "g.V().hasLabel('person')");

        queryPassthrough.verify(args);
    }

    @Test(expected = AthenaConnectorException.class)
    public void verify_WithEmptyQuery_ThrowsAthenaConnectorException() {
        Map<String, String> args = new HashMap<>();
        args.put("DATABASE", "testDb");
        args.put("COLLECTION", "testCollection");
        args.put("QUERY", "");

        queryPassthrough.verify(args);
    }

    @Test
    public void neptuneQueryPassthrough_WhenInstantiated_ImplementsQueryPassthroughSignature() {
        assertThat(queryPassthrough)
                .isInstanceOf(QueryPassthroughSignature.class);
    }

    @Test
    public void constantValues_WhenAccessed_ReturnExpectedStringValues() {
        assertEquals("query", NeptuneQueryPassthrough.NAME);
        assertEquals("system", NeptuneQueryPassthrough.SCHEMA_NAME);
        assertEquals("DATABASE", NeptuneQueryPassthrough.DATABASE);
        assertEquals("COLLECTION", NeptuneQueryPassthrough.COLLECTION);
        assertEquals("QUERY", NeptuneQueryPassthrough.QUERY);
    }
} 