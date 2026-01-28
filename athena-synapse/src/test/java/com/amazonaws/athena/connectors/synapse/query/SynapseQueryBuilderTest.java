/*-
 * #%L
 * athena-synapse
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
package com.amazonaws.athena.connectors.synapse.query;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import com.amazonaws.athena.connectors.synapse.SynapseMetadataHandler;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SynapseQueryBuilderTest
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
    private static final String PARTITION_COLUMN = "employee_id";

    private SynapseQueryFactory queryFactory;
    private Schema testSchema;
    private Split split;

    @Before
    public void setUp()
    {
        queryFactory = new SynapseQueryFactory();
        testSchema = createTestSchema();
        split = createSplit();
    }

    @Test
    public void getTemplateName_WhenCalled_ReturnsSelectQuery()
    {
        String templateName = SynapseQueryBuilder.getTemplateName();
        
        assertNotNull("Template name should not be null", templateName);
        assertEquals("Template name should match", "select_query", templateName);
    }

    @Test
    public void build_WithProjectionAndTableName_GeneratesCorrectSelectQuery()
    {
        SynapseQueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);

        String sql = builder.build();
        
        String expectedSql = "SELECT \"id\", \"name\", \"active\", \"score\" FROM \"test_schema\".\"test_table\"";
        assertEquals("Generated SQL should match expected query exactly", expectedSql, sql);
    }

    @Test
    public void build_WithOrderByClause_GeneratesQueryWithOrderBy()
    {
        List<OrderByField> orderByFields = createOrderByFields();
        Constraints constraints = createConstraints(orderByFields);

        SynapseQueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);
        builder.withOrderByClause(constraints);

        String sql = builder.build();
        
        String expectedSql = "SELECT \"id\", \"name\", \"active\", \"score\" FROM \"test_schema\".\"test_table\" ORDER BY \"name\" ASC NULLS FIRST, \"score\" DESC NULLS LAST";
        assertEquals("Generated SQL should match expected query with ORDER BY", expectedSql, sql);
    }

    @Test
    public void build_WithLimitClause_DoesNotIncludeLimitInSql()
    {
        // Synapse does not support LIMIT
        Constraints constraints = new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), 100, Collections.emptyMap(), null);
        SynapseQueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);
        builder.withLimitClause(constraints);

        String sql = builder.build();
        
        String expectedSql = "SELECT \"id\", \"name\", \"active\", \"score\" FROM \"test_schema\".\"test_table\"";
        assertEquals("Generated SQL should not include LIMIT (Synapse does not support it)", expectedSql, sql);
    }

    @Test
    public void build_WithEmptyProjection_GeneratesQueryWithNull()
    {
        Schema emptySchema = new Schema(Collections.emptyList());

        SynapseQueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withProjection(emptySchema, split);
        builder.withTableName(TEST_TABLE);

        String sql = builder.build();
        
        String expectedSql = "SELECT null FROM \"test_schema\".\"test_table\"";
        assertEquals("Generated SQL should contain SELECT null for empty projection", expectedSql, sql);
    }

    @Test
    public void getSchemaName_WhenCalled_ReturnsQuotedSchemaName()
    {
        SynapseQueryBuilder builder = queryFactory.createQueryBuilder();
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
        Constraints constraints = createConstraints(orderByFields);

        SynapseQueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withCatalog(null); // Synapse doesn't use catalog in FROM
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);
        builder.withOrderByClause(constraints);
        builder.withLimitClause(createConstraints(Collections.emptyList()));

        String sql = builder.build();
        
        String expectedSql = "SELECT \"id\", \"name\", \"active\", \"score\" FROM \"test_schema\".\"test_table\" ORDER BY \"name\" ASC NULLS FIRST, \"score\" DESC NULLS LAST";
        assertEquals("Generated SQL should include all components (projection, FROM, ORDER BY, no LIMIT)", expectedSql, sql);
    }

    @Test
    public void build_WithEmptySchema_GeneratesQueryWithTableNameOnly()
    {
        TableName tableWithEmptySchema = new TableName("", TEST_TABLE_NAME);

        SynapseQueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withProjection(testSchema, split);
        builder.withTableName(tableWithEmptySchema);

        String sql = builder.build();
        
        String expectedSql = "SELECT \"id\", \"name\", \"active\", \"score\" FROM \"\".\"test_table\"";
        assertEquals("Generated SQL should contain table name without schema prefix", expectedSql, sql);
    }

    @Test
    public void getSchemaName_WithQuotesInIdentifier_EscapesQuotes()
    {
        SynapseQueryBuilder builder = queryFactory.createQueryBuilder();
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

        SynapseQueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withProjection(testSchema, splitWithPartition);
        builder.withTableName(TEST_TABLE);

        List<String> projection = builder.getProjection();
        
        assertFalse("Partition column should be filtered out", projection.contains("\"employee_id\""));
        assertEquals("Projection should have 4 columns (excluding partition)", 4, projection.size());
    }

    @Test
    public void build_WithRangePartitionBothBoundaries_GeneratesPartitionInWhereClause()
    {
        Map<String, String> splitProperties = new HashMap<>();
        splitProperties.put(SynapseMetadataHandler.PARTITION_COLUMN, PARTITION_COLUMN);
        splitProperties.put(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM, "1000");
        splitProperties.put(SynapseMetadataHandler.PARTITION_BOUNDARY_TO, "2000");
        
        Split splitWithPartition = mock(Split.class);
        when(splitWithPartition.getProperties()).thenReturn(splitProperties);
        when(splitWithPartition.getProperty(SynapseMetadataHandler.PARTITION_COLUMN)).thenReturn(PARTITION_COLUMN);
        when(splitWithPartition.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM)).thenReturn("1000");
        when(splitWithPartition.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_TO)).thenReturn("2000");

        SynapseQueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withProjection(testSchema, splitWithPartition);
        builder.withTableName(TEST_TABLE);
        builder.withConjuncts(testSchema, createConstraints(Collections.emptyList()), splitWithPartition);

        String sql = builder.build();

        String expectedSql = "SELECT \"id\", \"name\", \"active\", \"score\", \"employee_id\" FROM \"test_schema\".\"test_table\"  WHERE employee_id > 1000 and employee_id <= 2000";
        assertEquals("Generated SQL should include partition range in WHERE clause", expectedSql, sql);
    }

    @Test
    public void build_WithRangePartitionOnlyUpperBoundary_GeneratesPartitionInWhereClause()
    {
        Map<String, String> splitProperties = new HashMap<>();
        splitProperties.put(SynapseMetadataHandler.PARTITION_COLUMN, PARTITION_COLUMN);
        splitProperties.put(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM, " ");
        splitProperties.put(SynapseMetadataHandler.PARTITION_BOUNDARY_TO, "1000");
        
        Split splitWithPartition = mock(Split.class);
        when(splitWithPartition.getProperties()).thenReturn(splitProperties);
        when(splitWithPartition.getProperty(SynapseMetadataHandler.PARTITION_COLUMN)).thenReturn(PARTITION_COLUMN);
        when(splitWithPartition.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM)).thenReturn(" ");
        when(splitWithPartition.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_TO)).thenReturn("1000");

        SynapseQueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withProjection(testSchema, splitWithPartition);
        builder.withTableName(TEST_TABLE);
        builder.withConjuncts(testSchema, createConstraints(Collections.emptyList()), splitWithPartition);

        String sql = builder.build();

        String expectedSql = "SELECT \"id\", \"name\", \"active\", \"score\", \"employee_id\" FROM \"test_schema\".\"test_table\"  WHERE employee_id <= 1000";
        assertEquals("Generated SQL should include only partition upper bound in WHERE clause", expectedSql, sql);
    }

    @Test
    public void build_WithRangePartitionOnlyLowerBoundary_GeneratesPartitionInWhereClause()
    {
        Map<String, String> splitProperties = new HashMap<>();
        splitProperties.put(SynapseMetadataHandler.PARTITION_COLUMN, PARTITION_COLUMN);
        splitProperties.put(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM, "4000");
        splitProperties.put(SynapseMetadataHandler.PARTITION_BOUNDARY_TO, " ");
        
        Split splitWithPartition = mock(Split.class);
        when(splitWithPartition.getProperties()).thenReturn(splitProperties);
        when(splitWithPartition.getProperty(SynapseMetadataHandler.PARTITION_COLUMN)).thenReturn(PARTITION_COLUMN);
        when(splitWithPartition.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM)).thenReturn("4000");
        when(splitWithPartition.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_TO)).thenReturn(" ");

        SynapseQueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withProjection(testSchema, splitWithPartition);
        builder.withTableName(TEST_TABLE);
        builder.withConjuncts(testSchema, createConstraints(Collections.emptyList()), splitWithPartition);

        String sql = builder.build();

        String expectedSql = "SELECT \"id\", \"name\", \"active\", \"score\", \"employee_id\" FROM \"test_schema\".\"test_table\"  WHERE employee_id > 4000";
        assertEquals("Generated SQL should include only partition lower bound in WHERE clause", expectedSql, sql);
    }

    @Test
    public void build_WithEmptyPartitionBoundaries_DoesNotIncludePartitionClause()
    {
        Map<String, String> splitProperties = new HashMap<>();
        splitProperties.put(SynapseMetadataHandler.PARTITION_COLUMN, PARTITION_COLUMN);
        splitProperties.put(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM, " ");
        splitProperties.put(SynapseMetadataHandler.PARTITION_BOUNDARY_TO, " ");
        
        Split splitWithEmptyBoundaries = mock(Split.class);
        when(splitWithEmptyBoundaries.getProperties()).thenReturn(splitProperties);
        when(splitWithEmptyBoundaries.getProperty(SynapseMetadataHandler.PARTITION_COLUMN)).thenReturn(PARTITION_COLUMN);
        when(splitWithEmptyBoundaries.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM)).thenReturn(" ");
        when(splitWithEmptyBoundaries.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_TO)).thenReturn(" ");

        SynapseQueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withProjection(testSchema, splitWithEmptyBoundaries);
        builder.withTableName(TEST_TABLE);
        builder.withConjuncts(testSchema, createConstraints(Collections.emptyList()), splitWithEmptyBoundaries);

        String sql = builder.build();

        String expectedSql = "SELECT \"id\", \"name\", \"active\", \"score\", \"employee_id\" FROM \"test_schema\".\"test_table\"";
        assertEquals("Generated SQL should not include partition clause when boundaries are empty", expectedSql, sql);
    }

    @Test(expected = NullPointerException.class)
    public void SynapseQueryBuilder_WhenCreatedWithNullTemplate_ThrowsNullPointerException()
    {
        new SynapseQueryBuilder(null);
    }

    @Test(expected = NullPointerException.class)
    public void build_WhenTableNameNotSet_ThrowsNullPointerException()
    {
        SynapseQueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withProjection(testSchema, split);
        builder.build();
    }

    @Test(expected = NullPointerException.class)
    public void build_WhenProjectionNotSet_ThrowsNullPointerException()
    {
        SynapseQueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withTableName(TEST_TABLE);
        builder.build();
    }

    @Test
    public void build_WithNullCatalog_DoesNotIncludeCatalogInFromClause()
    {
        SynapseQueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withCatalog(null);
        builder.withProjection(testSchema, split);
        builder.withTableName(TEST_TABLE);

        String sql = builder.build();
        
        String expectedSql = "SELECT \"id\", \"name\", \"active\", \"score\" FROM \"test_schema\".\"test_table\"";
        assertEquals("Generated SQL should not include catalog prefix (Synapse uses schema.table format)", expectedSql, sql);
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

    private Constraints createConstraints(List<OrderByField> orderByFields)
    {
        return new Constraints(new HashMap<>(), Collections.emptyList(), orderByFields, DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
    }
}
