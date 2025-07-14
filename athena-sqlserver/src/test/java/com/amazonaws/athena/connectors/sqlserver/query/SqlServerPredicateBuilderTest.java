/*-
 * #%L
 * athena-sqlserver
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
package com.amazonaws.athena.connectors.sqlserver.query;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SqlServerPredicateBuilderTest extends TestBase
{
    private SqlServerPredicateBuilder predicateBuilder;
    private static final String SQLSERVER_QUOTE_CHAR = "\"";

    @Before
    public void setup()
    {
        predicateBuilder = new SqlServerPredicateBuilder(SQLSERVER_QUOTE_CHAR);
    }

    @Test
    public void testConstructor()
    {
        assertNotNull(predicateBuilder);
    }

    @Test
    public void testBuildConjuncts()
    {
        List<Field> fields = Arrays.asList(
            createField("id", org.apache.arrow.vector.types.Types.MinorType.INT.getType()),
            createField("name", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType())
        );
        Constraints constraints = mock(Constraints.class);
        when(constraints.getSummary()).thenReturn(Collections.emptyMap());
        List<Object> parameterValues = new ArrayList<>();

        List<String> result = predicateBuilder.buildConjuncts(fields, constraints, parameterValues);

        assertNotNull(result);
        assertTrue(result.isEmpty()); // No constraints, so no conjuncts
    }

    @Test
    public void testBuildConjunctsWithConstraints()
    {
        List<Field> fields = Arrays.asList(
            createField("id", org.apache.arrow.vector.types.Types.MinorType.INT.getType())
        );
        Constraints constraints = mock(Constraints.class);
        ValueSet valueSet = mock(ValueSet.class);
        when(valueSet.isNone()).thenReturn(false);
        when(valueSet.isAll()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(false);
        
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("id", valueSet);
        when(constraints.getSummary()).thenReturn(summary);
        
        List<Object> parameterValues = new ArrayList<>();

        List<String> result = predicateBuilder.buildConjuncts(fields, constraints, parameterValues);

        assertNotNull(result);
    }

    @Test
    public void testConvertValueForDatabaseInt()
    {
        Object value = 42;
        org.apache.arrow.vector.types.pojo.ArrowType.Int intType = 
            new org.apache.arrow.vector.types.pojo.ArrowType.Int(32, true);

        Object result = predicateBuilder.convertValueForDatabase(value, intType);

        assertEquals(value, result);
    }

    @Test
    public void testConvertValueForDatabaseVarchar()
    {
        Object value = "test_string";
        org.apache.arrow.vector.types.pojo.ArrowType.Utf8 utf8Type = 
            new org.apache.arrow.vector.types.pojo.ArrowType.Utf8();

        Object result = predicateBuilder.convertValueForDatabase(value, utf8Type);

        assertEquals(value, result);
    }

    @Test
    public void testConvertValueForDatabaseDate()
    {
        Date dateValue = Date.valueOf("2023-01-15");
        org.apache.arrow.vector.types.pojo.ArrowType.Date dateType = 
            new org.apache.arrow.vector.types.pojo.ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY);

        Object result = predicateBuilder.convertValueForDatabase(dateValue, dateType);

        assertNotNull(result);
        // Based on the implementation, it should return the original Date object
        assertTrue("Expected Date, but got: " + result.getClass().getSimpleName(), result instanceof Date);
        assertEquals(dateValue, result);
    }

    @Test
    public void testConvertValueForDatabaseTimestamp()
    {
        Timestamp timestampValue = Timestamp.valueOf("2023-01-15 10:30:45");
        org.apache.arrow.vector.types.pojo.ArrowType.Timestamp timestampType = 
            new org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC");

        Object result = predicateBuilder.convertValueForDatabase(timestampValue, timestampType);

        assertNotNull(result);
        // Based on the implementation, it should return the original Timestamp object
        assertTrue("Expected Timestamp, but got: " + result.getClass().getSimpleName(), result instanceof Timestamp);
        assertEquals(timestampValue, result);
    }

    @Test
    public void testConvertValueForDatabaseLocalDateTime()
    {
        LocalDateTime localDateTime = LocalDateTime.of(2023, 1, 15, 10, 30, 45);
        org.apache.arrow.vector.types.pojo.ArrowType.Timestamp timestampType = 
            new org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC");

        Object result = predicateBuilder.convertValueForDatabase(localDateTime, timestampType);

        assertNotNull(result);
        // Based on the implementation, it should return a Timestamp object
        assertTrue("Expected Timestamp, but got: " + result.getClass().getSimpleName(), result instanceof Timestamp);
    }

    @Test
    public void testConvertValueForDatabaseArrowText()
    {
        org.apache.arrow.vector.util.Text textValue = new org.apache.arrow.vector.util.Text("test_text");
        org.apache.arrow.vector.types.pojo.ArrowType.Utf8 utf8Type = 
            new org.apache.arrow.vector.types.pojo.ArrowType.Utf8();

        Object result = predicateBuilder.convertValueForDatabase(textValue, utf8Type);

        assertNotNull(result);
        assertTrue(result instanceof String);
        assertEquals("test_text", result);
    }

    @Test
    public void testConvertValueForDatabaseNull()
    {
        org.apache.arrow.vector.types.pojo.ArrowType.Utf8 utf8Type = 
            new org.apache.arrow.vector.types.pojo.ArrowType.Utf8();

        Object result = predicateBuilder.convertValueForDatabase(null, utf8Type);

        assertNull(result);
    }

    @Test
    public void testCreateFederationExpressionParser()
    {
        FederationExpressionParser result = predicateBuilder.createFederationExpressionParser();

        assertNotNull(result);
        assertTrue(result instanceof com.amazonaws.athena.connectors.sqlserver.SqlServerFederationExpressionParser);
    }

    private Field createField(String name, org.apache.arrow.vector.types.pojo.ArrowType type)
    {
        return Field.nullable(name, type);
    }
} 
