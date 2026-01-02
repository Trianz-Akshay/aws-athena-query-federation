/*-
 * #%L
 * athena-hbase
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
package com.amazonaws.athena.connectors.hbase.qpt;

import com.amazonaws.athena.connector.lambda.metadata.optimizations.querypassthrough.QueryPassthroughSignature;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HbaseQueryPassthroughTest
{
    private static final String SYSTEM = "system";
    private static final String QUERY = "query";
    private static final String SYSTEM_QUERY = "SYSTEM.QUERY";
    private static final String DATABASE = "DATABASE";
    private static final String COLLECTION = "COLLECTION";
    private static final String FILTER = "FILTER";
    private static final String TEST_DB = "test_db";
    private static final String TEST_COLLECTION = "test_collection";
    private static final String TEST_FILTER = "test_filter";
    private static final String WRONG_SIGNATURE = "WRONG.SIGNATURE";
    private static final int EXPECTED_ARGUMENT_COUNT = 3;

    private final HbaseQueryPassthrough queryPassthrough = new HbaseQueryPassthrough();

    @Test
    public void getFunctionSchema_withDefault_returnsSystem()
    {
        assertEquals("Function schema should be system", SYSTEM, queryPassthrough.getFunctionSchema());
    }

    @Test
    public void getFunctionName_withDefault_returnsQuery()
    {
        assertEquals("Function name should be query", QUERY, queryPassthrough.getFunctionName());
    }

    @Test
    public void getFunctionSignature_withDefault_returnsSystemQuery()
    {
        assertEquals("Function signature should be SYSTEM.QUERY", SYSTEM_QUERY, queryPassthrough.getFunctionSignature());
    }

    @Test
    public void getFunctionArguments_withDefault_returnsThreeArguments()
    {
        List<String> arguments = queryPassthrough.getFunctionArguments();
        assertNotNull("Arguments should not be null", arguments);
        assertEquals("Should have 3 arguments", EXPECTED_ARGUMENT_COUNT, arguments.size());
        assertEquals("Arguments should match expected list", Arrays.asList(DATABASE, COLLECTION, FILTER), arguments);
    }

    @Test
    public void verify_withMissingDatabase_throwsIllegalArgumentException()
    {
        Map<String, String> engineQptArguments = new HashMap<>();
        engineQptArguments.put(QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, SYSTEM_QUERY);
        engineQptArguments.put(COLLECTION, TEST_COLLECTION);
        engineQptArguments.put(FILTER, TEST_FILTER);

        assertVerifyThrowsIllegalArgumentException(engineQptArguments);
    }

    @Test
    public void verify_withMissingCollection_throwsIllegalArgumentException()
    {
        Map<String, String> engineQptArguments = new HashMap<>();
        engineQptArguments.put(QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, SYSTEM_QUERY);
        engineQptArguments.put(DATABASE, TEST_DB);
        engineQptArguments.put(FILTER, TEST_FILTER);

        assertVerifyThrowsIllegalArgumentException(engineQptArguments);
    }

    @Test
    public void verify_withMissingFilter_throwsIllegalArgumentException()
    {
        Map<String, String> engineQptArguments = new HashMap<>();
        engineQptArguments.put(QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, SYSTEM_QUERY);
        engineQptArguments.put(DATABASE, TEST_DB);
        engineQptArguments.put(COLLECTION, TEST_COLLECTION);

        assertVerifyThrowsIllegalArgumentException(engineQptArguments);
    }

    @Test
    public void verify_withWrongFunctionSignature_throwsIllegalArgumentException()
    {
        Map<String, String> engineQptArguments = new HashMap<>();
        engineQptArguments.put(QueryPassthroughSignature.SCHEMA_FUNCTION_NAME, WRONG_SIGNATURE);
        engineQptArguments.put(DATABASE, TEST_DB);
        engineQptArguments.put(COLLECTION, TEST_COLLECTION);
        engineQptArguments.put(FILTER, TEST_FILTER);

        assertVerifyThrowsIllegalArgumentException(engineQptArguments);
    }

    @Test
    public void verify_withMissingFunctionSignature_throwsIllegalArgumentException()
    {
        Map<String, String> engineQptArguments = new HashMap<>();
        engineQptArguments.put(DATABASE, TEST_DB);
        engineQptArguments.put(COLLECTION, TEST_COLLECTION);
        engineQptArguments.put(FILTER, TEST_FILTER);

        assertVerifyThrowsIllegalArgumentException(engineQptArguments);
    }

    private void assertVerifyThrowsIllegalArgumentException(Map<String, String> engineQptArguments)
    {
        try {
            queryPassthrough.verify(engineQptArguments);
            fail("Expected IllegalArgumentException was not thrown");
        }
        catch (IllegalArgumentException ex) {
            assertNotNull("Exception should not be null", ex);
            assertTrue("Exception message should not be empty", 
                    ex.getMessage() != null && !ex.getMessage().isEmpty());
        }
    }
}

