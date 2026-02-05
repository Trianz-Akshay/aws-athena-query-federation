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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TeradataPredicateBuilderTest
{
    private static final ArrowType INT_TYPE = new ArrowType.Int(32, false);
    private static final ArrowType STRING_TYPE = new ArrowType.Utf8();

    private BlockAllocatorImpl allocator;
    private Split split;
    private List<TypeAndValue> parameterValues;
    private List<Field> fields;
    private Map<String, ValueSet> constraintMap;

    @Before
    public void setUp()
    {
        allocator = new BlockAllocatorImpl();
        split = mock(Split.class);
        when(split.getProperties()).thenReturn(Collections.emptyMap());
        parameterValues = new ArrayList<>();
        fields = new ArrayList<>();
        constraintMap = new LinkedHashMap<>();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void buildConjuncts_WithSingleValueRange_ReturnsEqualityPredicate()
    {
        ValueSet singleValueSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .build();
        constraintMap.put("intCol", singleValueSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain column name", conjuncts.get(0).contains("\"intCol\""));
        assertTrue("Conjunct should contain = operator", conjuncts.get(0).contains("="));
        assertEquals("Should have one parameter", 1, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithRangePredicate_ReturnsRangePredicate()
    {
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.above(allocator, INT_TYPE, 10), Marker.below(allocator, INT_TYPE, 20)))
                .build();
        constraintMap.put("intCol", rangeSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain > operator", conjuncts.get(0).contains(">"));
        assertTrue("Conjunct should contain < operator", conjuncts.get(0).contains("<"));
        assertEquals("Should have two parameters", 2, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithMultipleSingleValues_ReturnsInPredicate()
    {
        ValueSet inSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 20), Marker.exactly(allocator, INT_TYPE, 20)))
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 30), Marker.exactly(allocator, INT_TYPE, 30)))
                .build();
        constraintMap.put("intCol", inSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain IN", conjuncts.get(0).contains("IN"));
        assertTrue("Conjunct should contain parentheses", conjuncts.get(0).contains("("));
        assertEquals("Should have three parameters", 3, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithNullValueSet_ReturnsIsNullPredicate()
    {
        ValueSet nullSet = SortedRangeSet.newBuilder(INT_TYPE, true).build();
        constraintMap.put("intCol", nullSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain IS NULL", conjuncts.get(0).contains("IS NULL"));
        assertEquals("Should have no parameters", 0, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithUnboundedRange_ReturnsIsNotNullPredicate()
    {
        ValueSet notNullSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.lowerUnbounded(allocator, INT_TYPE), Marker.upperUnbounded(allocator, INT_TYPE)))
                .build();
        constraintMap.put("intCol", notNullSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain IS NOT NULL", conjuncts.get(0).contains("IS NOT NULL"));
        assertEquals("Should have no parameters", 0, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithPartitionColumn_FiltersOutPartitionColumn()
    {
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .build();
        constraintMap.put("intCol", rangeSet);
        constraintMap.put("partition", rangeSet);

        fields.add(Field.nullable("intCol", INT_TYPE));
        fields.add(Field.nullable("partition", INT_TYPE));

        Map<String, String> splitProperties = new HashMap<>();
        splitProperties.put("partition", "p0");
        Split splitWithPartition = mock(Split.class);
        when(splitWithPartition.getProperties()).thenReturn(splitProperties);

        List<String> conjuncts = buildConjuncts(constraintMap, fields, splitWithPartition);

        assertFalse("Should have at least one conjunct", conjuncts.isEmpty());
        assertTrue("Conjunct should contain intCol", conjuncts.get(0).contains("\"intCol\""));
    }

    @Test
    public void buildConjuncts_WithStringType_ReturnsStringPredicate()
    {
        ValueSet stringSet = SortedRangeSet.newBuilder(STRING_TYPE, false)
                .add(new Range(Marker.exactly(allocator, STRING_TYPE, "test"), Marker.exactly(allocator, STRING_TYPE, "test")))
                .build();
        constraintMap.put("stringCol", stringSet);

        fields.add(Field.nullable("stringCol", STRING_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain stringCol", conjuncts.get(0).contains("\"stringCol\""));
        assertEquals("Should have one parameter", 1, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithMultipleColumns_ReturnsMultipleConjuncts()
    {
        ValueSet intSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .build();
        ValueSet stringSet = SortedRangeSet.newBuilder(STRING_TYPE, false)
                .add(new Range(Marker.exactly(allocator, STRING_TYPE, "test"), Marker.exactly(allocator, STRING_TYPE, "test")))
                .build();
        constraintMap.put("intCol", intSet);
        constraintMap.put("stringCol", stringSet);

        fields.add(Field.nullable("intCol", INT_TYPE));
        fields.add(Field.nullable("stringCol", STRING_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have two conjuncts", 2, conjuncts.size());
        assertTrue("Should contain intCol conjunct", conjuncts.stream().anyMatch(c -> c.contains("\"intCol\"")));
        assertTrue("Should contain stringCol conjunct", conjuncts.stream().anyMatch(c -> c.contains("\"stringCol\"")));
        assertEquals("Should have two parameters", 2, parameterValues.size());
    }

    private List<String> buildConjuncts(Map<String, ValueSet> constraintMap, List<Field> fields, Split splitParam)
    {
        Constraints constraints = new Constraints(constraintMap, Collections.emptyList(),
                Collections.emptyList(), DEFAULT_NO_LIMIT, Collections.emptyMap(), null);
        TeradataPredicateBuilder predicateBuilder = new TeradataPredicateBuilder();
        List<String> conjuncts = predicateBuilder.buildConjuncts(fields, constraints, parameterValues, splitParam);
        assertNotNull("Conjuncts should not be null", conjuncts);
        return conjuncts;
    }
}
