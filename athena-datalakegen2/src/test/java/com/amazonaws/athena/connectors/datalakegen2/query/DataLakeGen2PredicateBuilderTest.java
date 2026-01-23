/*-
 * #%L
 * athena-datalakegen2
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
package com.amazonaws.athena.connectors.datalakegen2.query;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataLakeGen2PredicateBuilderTest
{
    private static final ArrowType INT_TYPE = new ArrowType.Int(32, false);
    private static final ArrowType STRING_TYPE = new ArrowType.Utf8();
    
    private BlockAllocatorImpl allocator;
    private DataLakeGen2PredicateBuilder predicateBuilder;
    private Split split;
    private Map<String, ValueSet> constraintMap;

    @Before
    public void setUp()
    {
        allocator = new BlockAllocatorImpl();
        predicateBuilder = new DataLakeGen2PredicateBuilder();
        split = mock(Split.class);
        when(split.getProperties()).thenReturn(Collections.emptyMap());
        constraintMap = new LinkedHashMap<>();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void buildConjuncts_WithEmptyConstraints_ReturnsEmptyList()
    {
        Schema schema = createSchema(Collections.emptyMap());
        Constraints constraints = createConstraints(Collections.emptyMap());
        
        List<TypeAndValue> parameterValues = new ArrayList<>();
        List<String> conjuncts = predicateBuilder.buildConjuncts(schema.getFields(), constraints, parameterValues, split);
        
        assertNotNull("Conjuncts should not be null", conjuncts);
        assertTrue("Conjuncts should be empty", conjuncts.isEmpty());
        assertTrue("Parameter values should be empty", parameterValues.isEmpty());
    }

    @Test
    public void buildConjuncts_WithRangeConstraint_GeneratesRangePredicate()
    {
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.above(allocator, INT_TYPE, 10), Marker.below(allocator, INT_TYPE, 20)))
                .build();
        constraintMap.put("intCol", rangeSet);

        Schema schema = createSchema(constraintMap);
        Constraints constraints = createConstraints(constraintMap);
        
        List<TypeAndValue> parameterValues = new ArrayList<>();
        List<String> conjuncts = predicateBuilder.buildConjuncts(schema.getFields(), constraints, parameterValues, split);
        
        assertNotNull("Conjuncts should not be null", conjuncts);
        assertTrue("Conjuncts should not be empty", !conjuncts.isEmpty());
        assertTrue("Conjunct should contain column name", conjuncts.get(0).contains("\"intCol\""));
        assertTrue("Conjunct should contain > operator", conjuncts.get(0).contains(">"));
        assertTrue("Conjunct should contain < operator", conjuncts.get(0).contains("<"));
        assertEquals("Should have 2 parameter values", 2, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithInConstraint_GeneratesInPredicate()
    {
        ValueSet inSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 20), Marker.exactly(allocator, INT_TYPE, 20)))
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 30), Marker.exactly(allocator, INT_TYPE, 30)))
                .build();
        constraintMap.put("intCol", inSet);

        Schema schema = createSchema(constraintMap);
        Constraints constraints = createConstraints(constraintMap);
        
        List<TypeAndValue> parameterValues = new ArrayList<>();
        List<String> conjuncts = predicateBuilder.buildConjuncts(schema.getFields(), constraints, parameterValues, split);
        
        assertNotNull("Conjuncts should not be null", conjuncts);
        assertTrue("Conjuncts should not be empty", !conjuncts.isEmpty());
        assertTrue("Conjunct should contain IN", conjuncts.get(0).contains("IN"));
        assertTrue("Conjunct should contain column name", conjuncts.get(0).contains("\"intCol\""));
        assertEquals("Should have 3 parameter values", 3, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithNullConstraint_GeneratesIsNullPredicate()
    {
        ValueSet nullSet = SortedRangeSet.newBuilder(INT_TYPE, true).build();
        constraintMap.put("intCol", nullSet);

        Schema schema = createSchema(constraintMap);
        Constraints constraints = createConstraints(constraintMap);
        
        List<TypeAndValue> parameterValues = new ArrayList<>();
        List<String> conjuncts = predicateBuilder.buildConjuncts(schema.getFields(), constraints, parameterValues, split);
        
        assertNotNull("Conjuncts should not be null", conjuncts);
        assertTrue("Conjuncts should not be empty", !conjuncts.isEmpty());
        assertTrue("Conjunct should contain IS NULL", conjuncts.get(0).contains("IS NULL"));
        assertTrue("Conjunct should contain column name", conjuncts.get(0).contains("\"intCol\""));
        assertTrue("Parameter values should be empty", parameterValues.isEmpty());
    }

    @Test
    public void buildConjuncts_WithNotNullConstraint_GeneratesIsNotNullPredicate()
    {
        ValueSet notNullSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.lowerUnbounded(allocator, INT_TYPE), Marker.upperUnbounded(allocator, INT_TYPE)))
                .build();
        constraintMap.put("intCol", notNullSet);

        Schema schema = createSchema(constraintMap);
        Constraints constraints = createConstraints(constraintMap);
        
        List<TypeAndValue> parameterValues = new ArrayList<>();
        List<String> conjuncts = predicateBuilder.buildConjuncts(schema.getFields(), constraints, parameterValues, split);
        
        assertNotNull("Conjuncts should not be null", conjuncts);
        assertTrue("Conjuncts should not be empty", !conjuncts.isEmpty());
        assertTrue("Conjunct should contain IS NOT NULL", conjuncts.get(0).contains("IS NOT NULL"));
        assertTrue("Conjunct should contain column name", conjuncts.get(0).contains("\"intCol\""));
        assertTrue("Parameter values should be empty", parameterValues.isEmpty());
    }

    @Test
    public void buildConjuncts_WithMultipleColumns_GeneratesMultiplePredicates()
    {
        ValueSet intSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .build();
        ValueSet stringSet = SortedRangeSet.newBuilder(STRING_TYPE, false)
                .add(new Range(Marker.exactly(allocator, STRING_TYPE, "test"), Marker.exactly(allocator, STRING_TYPE, "test")))
                .build();
        constraintMap.put("intCol", intSet);
        constraintMap.put("stringCol", stringSet);

        Schema schema = createSchema(constraintMap);
        Constraints constraints = createConstraints(constraintMap);
        
        List<TypeAndValue> parameterValues = new ArrayList<>();
        List<String> conjuncts = predicateBuilder.buildConjuncts(schema.getFields(), constraints, parameterValues, split);
        
        assertNotNull("Conjuncts should not be null", conjuncts);
        assertTrue("Should have multiple conjuncts", conjuncts.size() >= 2);
        assertEquals("Should have 2 parameter values", 2, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithPartitionColumn_FiltersOutPartitionColumn()
    {
        ValueSet intSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .build();
        constraintMap.put("intCol", intSet);

        Schema schema = createSchema(constraintMap);
        Constraints constraints = createConstraints(constraintMap);
        
        // Create split with partition column
        Map<String, String> splitProperties = new HashMap<>();
        splitProperties.put("intCol", "p0");
        Split splitWithPartition = mock(Split.class);
        when(splitWithPartition.getProperties()).thenReturn(splitProperties);
        
        List<TypeAndValue> parameterValues = new ArrayList<>();
        List<String> conjuncts = predicateBuilder.buildConjuncts(schema.getFields(), constraints, parameterValues, splitWithPartition);
        
        // Partition column should be filtered out, so no conjuncts should be generated
        assertTrue("Conjuncts should be empty for partition column", conjuncts.isEmpty());
    }

    private Constraints createConstraints(Map<String, ValueSet> constraintMap)
    {
        return new Constraints(constraintMap, Collections.emptyList(), 
                Collections.emptyList(), 0, Collections.emptyMap(), null);
    }

    private Schema createSchema(Map<String, ValueSet> constraintMap)
    {
        List<Field> fields = new ArrayList<>();
        for (String columnName : constraintMap.keySet()) {
            ArrowType type = constraintMap.get(columnName).getType();
            fields.add(Field.nullable(columnName, type));
        }
        return new Schema(fields);
    }
}
