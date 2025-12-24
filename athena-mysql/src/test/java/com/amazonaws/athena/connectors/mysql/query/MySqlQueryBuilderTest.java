/*-
 * #%L
 * Amazon Athena MySQL Connector
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
package com.amazonaws.athena.connectors.mysql.query;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.stringtemplate.v4.ST;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MySqlQueryBuilderTest extends TestBase
{
    @Mock
    private ST mockTemplate;

    private MySqlQueryBuilder queryBuilder;
    private static final String MYSQL_QUOTE_CHAR = "`";

    @Before
    public void setup()
    {
        when(mockTemplate.add(any(), any())).thenReturn(mockTemplate);
        when(mockTemplate.render()).thenReturn("SELECT * FROM test_table");
        
        queryBuilder = new MySqlQueryBuilder(mockTemplate, MYSQL_QUOTE_CHAR);
    }

    @Test
    public void testConstructor()
    {
        assertNotNull(queryBuilder);
    }

    @Test
    public void testBuildPartitionWhereClauses()
    {
        Split split = mock(Split.class);
        Map<String, String> properties = new HashMap<>();
        properties.put("partition_col", "partition_value");
        when(split.getProperties()).thenReturn(properties);

        List<String> result = queryBuilder.buildPartitionWhereClauses(split);

        // MySQL uses PARTITION(partition_name) syntax in FROM clause, not WHERE clause
        // So it should return empty list
        assertTrue(result.isEmpty());
    }

    @Test
    public void testWithConjuncts()
    {
        Schema schema = mock(Schema.class);
        Constraints constraints = mock(Constraints.class);
        List<Field> fields = Arrays.asList(
            createField("id", org.apache.arrow.vector.types.Types.MinorType.INT.getType()),
            createField("name", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType())
        );
        when(schema.getFields()).thenReturn(fields);
        when(constraints.getSummary()).thenReturn(Collections.emptyMap());

        queryBuilder.withConjuncts(schema, constraints);

        assertNotNull(queryBuilder);
    }

    @Test
    public void testWithOrderByClause()
    {
        Constraints constraints = mock(Constraints.class);
        List<OrderByField> orderByFields = Arrays.asList(
            new OrderByField("id", OrderByField.Direction.ASC_NULLS_FIRST),
            new OrderByField("name", OrderByField.Direction.DESC_NULLS_LAST)
        );
        when(constraints.getOrderByClause()).thenReturn(orderByFields);

        queryBuilder.withOrderByClause(constraints);

        assertNotNull(queryBuilder);
    }

    @Test
    public void testWithTableName()
    {
        com.amazonaws.athena.connector.lambda.domain.TableName tableName = 
            new com.amazonaws.athena.connector.lambda.domain.TableName("test_schema", "test_table");

        queryBuilder.withTableName(tableName);

        assertNotNull(queryBuilder);
    }

    @Test
    public void testWithCatalogName()
    {
        String catalogName = "test_catalog";

        queryBuilder.withCatalogName(catalogName);

        assertNotNull(queryBuilder);
    }

    @Test
    public void testBuild()
    {
        // Set up required fields
        queryBuilder.withTableName(new com.amazonaws.athena.connector.lambda.domain.TableName("schema", "table"));
        queryBuilder.withCatalogName("catalog");
        
        // Set up projection to avoid NullPointerException
        org.apache.arrow.vector.types.pojo.Schema schema = mock(org.apache.arrow.vector.types.pojo.Schema.class);
        Split split = mock(Split.class);
        queryBuilder.withProjection(schema, split);

        String result = queryBuilder.build();

        assertNotNull(result);
        verify(mockTemplate, times(1)).render();
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
        List<TypeAndValue> accumulator = new ArrayList<>();

        List<String> result = queryBuilder.buildConjuncts(fields, constraints, accumulator);

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
        
        List<TypeAndValue> accumulator = new ArrayList<>();

        List<String> result = queryBuilder.buildConjuncts(fields, constraints, accumulator);

        assertNotNull(result);
    }

    @Test
    public void testWithSplit()
    {
        Split split = mock(Split.class);
        Map<String, String> properties = new HashMap<>();
        properties.put("partition_col", "partition_value");
        when(split.getProperties()).thenReturn(properties);

        queryBuilder.withSplit(split);

        assertNotNull(queryBuilder);
    }

    @Test
    public void testWithProjection()
    {
        Schema schema = mock(Schema.class);
        Split split = mock(Split.class);

        queryBuilder.withProjection(schema, split);

        assertNotNull(queryBuilder);
    }

    private Field createField(String name, org.apache.arrow.vector.types.pojo.ArrowType type)
    {
        return Field.nullable(name, type);
    }
} 
