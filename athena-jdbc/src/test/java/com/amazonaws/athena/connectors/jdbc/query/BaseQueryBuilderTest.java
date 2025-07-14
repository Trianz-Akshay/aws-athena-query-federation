/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.query;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.stringtemplate.v4.ST;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class BaseQueryBuilderTest extends TestBase
{
    private TestBaseQueryBuilder queryBuilder;
    private ST mockTemplate;
    private Schema mockSchema;
    private Split mockSplit;
    private TableName mockTableName;
    private Constraints mockConstraints;

    @Before
    public void setup()
    {
        mockTemplate = Mockito.mock(ST.class);
        when(mockTemplate.add(any(), any())).thenReturn(mockTemplate);
        when(mockTemplate.render()).thenReturn("SELECT * FROM test_table");
        
        queryBuilder = new TestBaseQueryBuilder(mockTemplate, "\"");
        
        // Setup mock schema
        List<Field> fields = Arrays.asList(
            Field.nullable("id", org.apache.arrow.vector.types.Types.MinorType.INT.getType()),
            Field.nullable("name", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()),
            Field.nullable("value", org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType())
        );
        mockSchema = new Schema(fields);
        
        // Setup mock split
        Map<String, String> splitProperties = new HashMap<>();
        splitProperties.put("partition_col", "partition_value");
        mockSplit = Mockito.mock(Split.class);
        when(mockSplit.getProperties()).thenReturn(splitProperties);
        
        // Setup mock table name
        mockTableName = new TableName("test_schema", "test_table");
        
        // Setup mock constraints
        mockConstraints = Mockito.mock(Constraints.class);
        when(mockConstraints.getSummary()).thenReturn(Collections.emptyMap());
    }

    @Test
    public void testWithProjection()
    {
        BaseQueryBuilder result = queryBuilder.withProjection(mockSchema, mockSplit);
        
        assertEquals(queryBuilder, result);
        assertNotNull(queryBuilder.getProjection());
        assertEquals(3, queryBuilder.getProjection().size()); // All fields should be included
        assertTrue(queryBuilder.getProjection().contains("id"));
        assertTrue(queryBuilder.getProjection().contains("name"));
        assertTrue(queryBuilder.getProjection().contains("value"));
    }

    @Test
    public void testWithTableName()
    {
        BaseQueryBuilder result = queryBuilder.withTableName(mockTableName);
        
        assertEquals(queryBuilder, result);
        assertEquals("test_schema", queryBuilder.getSchemaName());
        assertEquals("test_table", queryBuilder.getTableName());
    }

    @Test
    public void testWithSplit()
    {
        BaseQueryBuilder result = queryBuilder.withSplit(mockSplit);
        
        assertEquals(queryBuilder, result);
        // The split should be processed correctly
        assertNotNull(result);
    }

    @Test
    public void testWithCatalogName()
    {
        BaseQueryBuilder result = queryBuilder.withCatalogName("test_catalog");
        
        assertEquals(queryBuilder, result);
        assertEquals("test_catalog", queryBuilder.getCatalogName());
    }

    @Test
    public void testWithConjuncts()
    {
        BaseQueryBuilder result = queryBuilder.withConjuncts(mockSchema, mockConstraints);
        
        assertEquals(queryBuilder, result);
        assertNotNull(queryBuilder.getConjuncts());
    }

    @Test
    public void testWithOrderByClause()
    {
        List<OrderByField> orderByFields = Arrays.asList(
            new OrderByField("id", OrderByField.Direction.ASC_NULLS_FIRST)
        );
        when(mockConstraints.getOrderByClause()).thenReturn(orderByFields);
        
        BaseQueryBuilder result = queryBuilder.withOrderByClause(mockConstraints);
        
        assertEquals(queryBuilder, result);
        assertNotNull(queryBuilder.getOrderByClause());
    }

    @Test
    public void testWithLimitClause()
    {
        when(mockConstraints.getLimit()).thenReturn(100L);
        
        BaseQueryBuilder result = queryBuilder.withLimitClause(mockConstraints);
        
        assertEquals(queryBuilder, result);
        assertEquals("LIMIT 100", queryBuilder.getLimitClause());
    }

    @Test
    public void testBuild()
    {
        queryBuilder.withTableName(mockTableName);
        queryBuilder.withProjection(mockSchema, mockSplit);
        
        String result = queryBuilder.build();
        
        assertNotNull(result);
        assertEquals("SELECT * FROM test_table", result);
    }

    @Test
    public void testQuote()
    {
        String result = queryBuilder.quote("test_column");
        assertEquals("\"test_column\"", result);
    }

    @Test
    public void testGetParameterValues()
    {
        List<Object> result = queryBuilder.getParameterValues();
        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testGetTemplateName()
    {
        String result = BaseQueryBuilder.getTemplateName();
        assertEquals("select_query", result);
    }

    // Test implementation of BaseQueryBuilder for testing
    private static class TestBaseQueryBuilder extends BaseQueryBuilder
    {
        public TestBaseQueryBuilder(ST template, String quoteChar)
        {
            super(template, quoteChar);
        }

        @Override
        protected List<String> buildConjuncts(List<Field> fields, Constraints constraints, List<Object> parameterValues)
        {
            return Collections.emptyList();
        }
    }
} 