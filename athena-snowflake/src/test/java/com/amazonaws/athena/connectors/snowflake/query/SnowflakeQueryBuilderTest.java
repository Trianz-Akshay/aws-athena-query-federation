/*-
 * #%L
 * athena-snowflake
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.snowflake.query;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnowflakeQueryBuilderTest
{
    private static final String TEST_SCHEMA_NAME = "test_schema";
    private static final String TEST_TABLE_NAME = "test_table";
    private static final TableName TEST_TABLE = new TableName(TEST_SCHEMA_NAME, TEST_TABLE_NAME);
    
    private static final ArrowType STRING_TYPE = new ArrowType.Utf8();
    private static final ArrowType BIGINT_TYPE = new ArrowType.Int(64, false);
    private static final ArrowType BOOLEAN_TYPE = ArrowType.Bool.INSTANCE;

    private static final String COLUMN_ID = "id";
    private static final String COLUMN_NAME = "name";
    private static final String COLUMN_ACTIVE = "active";
    private static final String COLUMN_SCORE = "score";
    private static final String PARTITION_COLUMN = "partition_col";

    private static final String SQL_SHOULD_CONTAIN_SELECT = "SQL should contain SELECT";
    private static final String SQL_SHOULD_CONTAIN_ALL_COLUMNS = "SQL should contain all columns";
    private static final String SQL_SHOULD_CONTAIN_FROM_CLAUSE = "SQL should contain FROM clause";

    private SnowflakeQueryFactory queryFactory;
    private Schema testSchema;
    private Split split;
    private SnowflakeQueryBuilder builder;

    @Before
    public void setUp()
    {
        queryFactory = new SnowflakeQueryFactory();
        testSchema = createTestSchema();
        split = createSplit();
        builder = queryFactory.createQueryBuilder();
    }

    @Test
    public void getTemplateName_WhenCalled_ReturnsSelectQuery()
    {
        String templateName = SnowflakeQueryBuilder.getTemplateName();
        
        assertNotNull("Template name should not be null", templateName);
        assertEquals("Template name should match", "select_query", templateName);
    }

    @Test
    public void build_WithProjectionAndTableName_GeneratesSelectQueryWithAllColumnsFromSchemaTableAndExcludesPartitionColumn()
    {
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);

        String sql = builder.build();
        
        assertNotNull("SQL should not be null", sql);
        // Verify all SELECT query components in a single comprehensive check
        assertTrue("SQL should contain SELECT, all columns, FROM clause, and exclude partition column",
                sql.contains("SELECT") &&
                sql.contains("\"id\"") &&
                sql.contains("\"name\"") &&
                sql.contains("\"active\"") &&
                sql.contains("\"score\"") &&
                sql.contains("FROM \"test_schema\".\"test_table\"") &&
                !sql.contains("\"partition_col\""));
    }

    @Test
    public void build_WithOrderByClause_GeneratesQueryWithOrderBy()
    {
        List<OrderByField> orderByFields = createOrderByFields();
        Constraints constraints = createConstraintsWithOrderBy(orderByFields);

        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);
        builder.withOrderByClause(constraints);

        String sql = builder.build();
        
        assertNotNull("SQL should not be null", sql);
        assertTrue("SQL should contain ORDER BY clause", sql.contains("ORDER BY"));
        assertTrue("SQL should contain name ordering", sql.contains("\"name\" ASC NULLS FIRST"));
        assertTrue("SQL should contain score ordering", sql.contains("\"score\" DESC NULLS LAST"));
    }

    @Test
    public void build_WithLimitClause_IncludesLimitInSql()
    {
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);
        Constraints constraints = new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), 100, Collections.emptyMap(), null);
        builder.withLimitClause(constraints);

        String sql = builder.build();
        
        assertNotNull("SQL should not be null", sql);
        assertTrue("SQL should contain LIMIT clause", sql.contains("LIMIT 100"));
    }

    @Test
    public void build_WithEmptyProjection_GeneratesQueryWithNull()
    {
        Schema emptySchema = new Schema(Collections.emptyList());

        builder.withProjection(emptySchema, split);
        builder.withTableName(TEST_TABLE);

        String sql = builder.build();
        
        assertNotNull("SQL should not be null", sql);
        assertTrue(SQL_SHOULD_CONTAIN_SELECT, sql.contains("SELECT"));
        assertTrue("SQL should contain null for empty projection", sql.contains("null"));
        assertTrue(SQL_SHOULD_CONTAIN_FROM_CLAUSE, sql.contains("FROM \"test_schema\".\"test_table\""));
    }

    @Test
    public void getSchemaName_WhenCalled_ReturnsQuotedSchemaName()
    {
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);

        assertEquals("Schema name should match", "\"test_schema\"", builder.getSchemaName());
        assertEquals("Table name should match", "\"test_table\"", builder.getTableName());
        assertEquals("Projection should have correct number of columns", 4, builder.getProjection().size());
        assertTrue("Projection should contain id", builder.getProjection().contains("\"id\""));
        assertTrue("Projection should contain name", builder.getProjection().contains("\"name\""));
        assertTrue("Projection should contain active", builder.getProjection().contains("\"active\""));
        assertTrue("Projection should contain score", builder.getProjection().contains("\"score\""));

        List<TypeAndValue> parameters = builder.getParameterValues();
        assertNotNull("Parameters should not be null", parameters);
    }

    @Test
    public void build_WithAllComponents_GeneratesCompleteQuery()
    {
        List<OrderByField> orderByFields = createOrderByFields();
        Constraints constraints = createConstraintsWithOrderBy(orderByFields);

        builder.withCatalog(null); // Snowflake doesn't use catalog in FROM clause
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);
        builder.withOrderByClause(constraints);
        Constraints limitConstraints = new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), 50, Collections.emptyMap(), null);
        builder.withLimitClause(limitConstraints);

        String sql = builder.build();
        
        assertNotNull("SQL should not be null", sql);
        assertTrue(SQL_SHOULD_CONTAIN_SELECT, sql.contains("SELECT"));
        assertTrue("SQL should contain schema name", sql.contains("test_schema"));
        assertTrue("SQL should contain table name", sql.contains("test_table"));
        assertTrue(SQL_SHOULD_CONTAIN_ALL_COLUMNS, sql.contains("\"id\""));
        assertTrue("SQL should contain ORDER BY", sql.contains("ORDER BY"));
        assertTrue("SQL should contain LIMIT", sql.contains("LIMIT 50"));
    }

    @Test
    public void build_WithEmptySchema_GeneratesQueryWithTableNameOnly()
    {
        TableName tableWithEmptySchema = new TableName("", TEST_TABLE_NAME);

        builder.withProjection(testSchema, split);
        builder.withTableName(tableWithEmptySchema);

        String sql = builder.build();
        
        assertNotNull("SQL should not be null", sql);
        assertTrue("SQL should contain table name", sql.contains("\"test_table\""));
    }

    @Test
    public void getSchemaName_WithQuotesInIdentifier_EscapesQuotes()
    {
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);

        // Test getters return quoted identifiers
        assertEquals("Schema name should be quoted", "\"test_schema\"", builder.getSchemaName());
        assertEquals("Table name should be quoted", "\"test_table\"", builder.getTableName());
        
        // Test that quotes are escaped in identifiers
        TableName tableWithQuotes = new TableName("schema\"name", "table\"name");
        builder.withTableName(tableWithQuotes);
        String schemaName = builder.getSchemaName();
        assertTrue("Quotes should be escaped", schemaName.contains("\"\""));
    }

    @Test
    public void withProjection_WithPartitionColumn_FiltersOutPartitionColumn()
    {
        // Create split with partition column
        Map<String, String> splitProperties = new HashMap<>();
        splitProperties.put(PARTITION_COLUMN, "p0");
        Split splitWithPartition = mock(Split.class);
        when(splitWithPartition.getProperties()).thenReturn(splitProperties);

        builder.withProjection(testSchema, splitWithPartition);
        builder.withTableName(TEST_TABLE);

        List<String> projection = builder.getProjection();
        
        assertFalse("Partition column should be filtered out", projection.contains("\"partition_col\""));
        assertEquals("Projection should have 4 columns (excluding partition)", 4, projection.size());
    }

    @Test(expected = NullPointerException.class)
    public void SnowflakeQueryBuilder_WhenCreatedWithNullTemplate_ThrowsNullPointerException()
    {
        new SnowflakeQueryBuilder(null);
    }

    @Test(expected = NullPointerException.class)
    public void build_WhenTableNameNotSet_ThrowsNullPointerException()
    {
        builder.withProjection(testSchema, split);
        builder.build();
    }

    @Test(expected = NullPointerException.class)
    public void build_WhenProjectionNotSet_ThrowsNullPointerException()
    {
        builder.withTableName(TEST_TABLE);
        builder.build();
    }

    @Test
    public void build_WithNullCatalog_DoesNotIncludeCatalogInFromClause()
    {
        builder.withCatalog(null);
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);

        String sql = builder.build();
        
        assertNotNull("SQL should not be null", sql);
        // Should not contain catalog in FROM clause
        assertFalse("SQL should not contain catalog", sql.contains("catalog"));
    }

    private Schema createTestSchema()
    {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field(COLUMN_ID, new FieldType(true, BIGINT_TYPE, null), null));
        fields.add(new Field(COLUMN_NAME, new FieldType(true, STRING_TYPE, null), null));
        fields.add(new Field(COLUMN_ACTIVE, new FieldType(true, BOOLEAN_TYPE, null), null));
        fields.add(new Field(COLUMN_SCORE, new FieldType(true, BIGINT_TYPE, null), null));
        fields.add(new Field(PARTITION_COLUMN, new FieldType(true, BIGINT_TYPE, null), null));
        return new Schema(fields);
    }

    private Split createSplit()
    {
        Split split = mock(Split.class);
        Map<String, String> properties = new HashMap<>();
        properties.put(PARTITION_COLUMN, "p0");
        when(split.getProperties()).thenReturn(properties);
        return split;
    }

    private List<OrderByField> createOrderByFields()
    {
        List<OrderByField> orderByFields = new ArrayList<>();
        orderByFields.add(new OrderByField(COLUMN_NAME, OrderByField.Direction.ASC_NULLS_FIRST));
        orderByFields.add(new OrderByField(COLUMN_SCORE, OrderByField.Direction.DESC_NULLS_LAST));
        return orderByFields;
    }

    private Constraints createConstraintsWithOrderBy(List<OrderByField> orderByFields)
    {
        return new Constraints(new HashMap<>(), Collections.emptyList(), orderByFields, DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
    }
}
