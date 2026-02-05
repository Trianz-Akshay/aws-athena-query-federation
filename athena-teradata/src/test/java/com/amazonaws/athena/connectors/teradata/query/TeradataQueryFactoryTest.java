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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TeradataQueryFactoryTest
{
    private static final TableName TEST_TABLE = new TableName("test_schema", "test_table");
    private TeradataQueryFactory queryFactory;
    private Schema testSchema;
    private Split split;

    @Before
    public void setUp()
    {
        queryFactory = new TeradataQueryFactory();
        testSchema = createTestSchema();
        split = createSplit();
    }

    @Test
    public void createQueryBuilder_whenCalled_returnsNonNullBuilder()
    {
        TeradataQueryBuilder builder = queryFactory.createQueryBuilder();
        assertNotNull("Query builder should not be null", builder);
    }

    @Test
    public void createQueryBuilder_whenBuilt_returnsBuildableQuery()
    {
        TeradataQueryBuilder builder = queryFactory.createQueryBuilder();
        builder.withCatalog("test_catalog");
        builder.withTableName(TEST_TABLE);
        builder.withProjection(testSchema, split);
        builder.withLimitClause(new Constraints(new HashMap<>(), Collections.emptyList(), Collections.emptyList(), 0L, Collections.emptyMap(), null));

        String sql = builder.build();

        assertNotNull("SQL should not be null", sql);
        assertTrue("SQL should contain SELECT", sql.contains("SELECT"));
        assertTrue("SQL should contain FROM", sql.contains("FROM"));
        assertTrue("SQL should contain table name", sql.contains("test_table"));
    }

    @Test
    public void getQueryTemplate_whenSelectQueryRequested_returnsSelectQueryTemplate()
    {
        assertNotNull("Template should not be null", queryFactory.getQueryTemplate(TeradataQueryBuilder.getTemplateName()));
    }

    private Schema createTestSchema()
    {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field("id", new FieldType(true, new ArrowType.Int(64, false), null), null));
        fields.add(new Field("name", new FieldType(true, new ArrowType.Utf8(), null), null));
        return new Schema(fields);
    }

    private Split createSplit()
    {
        Split split = mock(Split.class);
        Map<String, String> properties = new HashMap<>();
        when(split.getProperties()).thenReturn(properties);
        return split;
    }
}
