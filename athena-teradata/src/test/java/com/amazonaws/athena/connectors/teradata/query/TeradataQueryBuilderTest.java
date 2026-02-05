/*-
 * #%L
 * athena-teradata
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
package com.amazonaws.athena.connectors.teradata.query;

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

public class TeradataQueryBuilderTest
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
    private static final String PARTITION_COLUMN = "partition";

    private static final String SQL_SHOULD_CONTAIN_SELECT = "SQL should contain SELECT";
    private static final String SQL_SHOULD_CONTAIN_FROM_CLAUSE = "SQL should contain FROM clause";

    private TeradataQueryFactory queryFactory;
    private TeradataQueryBuilder builder;
    private Schema testSchema;
    private Split split;

    @Before
    public void setUp()
    {
        queryFactory = new TeradataQueryFactory();
        builder = queryFactory.createQueryBuilder();
        testSchema = createTestSchema();
        split = createSplitWithPartitionValue("p0");
    }

    @Test
    public void getTemplateName_WhenCalled_ReturnsSelectQuery()
    {
        String templateName = TeradataQueryBuilder.getTemplateName();
        assertNotNull("Template name should not be null", templateName);
        assertEquals("Template name should match", "select_query", templateName);
    }

    @Test
    public void build_WithProjectionAndTableName_GeneratesCorrectSelectQuery()
    {
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);

        String sql = builder.build();

        assertEquals("SELECT \"id\", \"name\", \"active\", \"score\" FROM \"test_schema\".\"test_table\"", sql);
    }

    @Test
    public void build_WithCatalog_IncludesCatalogInFromClause()
    {
        builder.withCatalog("test_catalog");
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);

        String sql = builder.build();

        assertNotNull("SQL should not be null", sql);
        assertTrue("SQL should contain catalog", sql.contains("\"test_catalog\""));
        assertTrue("SQL should contain schema.table", sql.contains("\"test_schema\".\"test_table\""));
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
    public void build_WithLimitClause_DoesNotIncludeLimitInSql()
    {
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);
        builder.withLimitClause(createConstraintsWithOrderBy(Collections.emptyList()));

        String sql = builder.build();

        assertNotNull("SQL should not be null", sql);
        assertFalse("Teradata does not support LIMIT; SQL should not contain LIMIT", sql.contains("LIMIT"));
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

        List<TypeAndValue> parameters = builder.getParameterValues();
        assertNotNull("Parameters should not be null", parameters);
    }

    @Test
    public void build_WithPartition_GeneratesOrOmitsPartitionClauseByValue()
    {
        // With partition value "p0" -> partition clause is included
        Split splitWithPartition = createSplitWithPartitionValue("p0");
        builder.withProjection(testSchema, splitWithPartition);
        builder.withTableName(TEST_TABLE);
        builder.withConjuncts(testSchema, createConstraintsWithOrderBy(Collections.emptyList()), splitWithPartition);
        String sqlWithPartition = builder.build();
        assertEquals("SELECT \"id\", \"name\", \"active\", \"score\" FROM \"test_schema\".\"test_table\"  WHERE partition = p0", sqlWithPartition);

        // With partition value "*" -> no partition clause (use fresh builder to avoid shared state)
        TeradataQueryBuilder builderForStar = queryFactory.createQueryBuilder();
        Split splitWithStar = createSplitWithPartitionValue("*");
        builderForStar.withProjection(testSchema, splitWithStar);
        builderForStar.withTableName(TEST_TABLE);
        builderForStar.withConjuncts(testSchema, createConstraintsWithOrderBy(Collections.emptyList()), splitWithStar);
        String sqlWithStar = builderForStar.build();
        assertEquals("SELECT \"id\", \"name\", \"active\", \"score\" FROM \"test_schema\".\"test_table\"", sqlWithStar);
    }

    @Test
    public void withProjection_WithPartitionColumn_FiltersOutPartitionColumn()
    {
        Map<String, String> splitProperties = new HashMap<>();
        splitProperties.put(PARTITION_COLUMN, "p0");
        Split splitWithPartition = mock(Split.class);
        when(splitWithPartition.getProperties()).thenReturn(splitProperties);

        builder.withProjection(testSchema, splitWithPartition);
        builder.withTableName(TEST_TABLE);

        List<String> projection = builder.getProjection();

        assertFalse("Partition column should be filtered out", projection.contains("\"partition\""));
        assertEquals("Projection should have 4 columns (excluding partition)", 4, projection.size());
    }

    @Test(expected = NullPointerException.class)
    public void TeradataQueryBuilder_WhenCreatedWithNullTemplate_ThrowsNullPointerException()
    {
        new TeradataQueryBuilder(null);
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
        assertTrue("SQL should contain FROM", sql.contains("FROM"));
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

    private Split createSplitWithPartitionValue(String partitionValue)
    {
        Split split = mock(Split.class);
        Map<String, String> properties = new HashMap<>();
        properties.put(PARTITION_COLUMN, partitionValue);
        when(split.getProperties()).thenReturn(properties);
        when(split.getProperty(PARTITION_COLUMN)).thenReturn(partitionValue);
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
