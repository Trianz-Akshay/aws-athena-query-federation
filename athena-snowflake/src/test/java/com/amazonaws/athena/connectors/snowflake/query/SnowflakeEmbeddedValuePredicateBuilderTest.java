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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnowflakeEmbeddedValuePredicateBuilderTest
{
    private static final ArrowType INT_TYPE = new ArrowType.Int(32, false);
    private static final ArrowType STRING_TYPE = new ArrowType.Utf8();
    private static final ArrowType BOOLEAN_TYPE = ArrowType.Bool.INSTANCE;
    private static final ArrowType DECIMAL_TYPE = new ArrowType.Decimal(10, 2, 128);
    private static final ArrowType FLOAT_TYPE = new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
    private static final ArrowType DATE_TYPE = new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY);
    
    private BlockAllocatorImpl allocator;
    private Split split;
    private List<TypeAndValue> parameterValues;
    private List<Field> fields;
    private SnowflakeEmbeddedValuePredicateBuilder builder;

    @Before
    public void setUp()
    {
        allocator = new BlockAllocatorImpl();
        split = mock(Split.class);
        when(split.getProperties()).thenReturn(Collections.emptyMap());
        parameterValues = new ArrayList<>();
        fields = new ArrayList<>();
        builder = new SnowflakeEmbeddedValuePredicateBuilder();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void constructor_WhenCalled_CreatesInstance()
    {
        SnowflakeEmbeddedValuePredicateBuilder instance = new SnowflakeEmbeddedValuePredicateBuilder();
        assertNotNull("Builder should not be null", instance);
    }

    @Test
    public void buildConjuncts_WithSingleValueRange_ReturnsEqualityPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet singleValueSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .build();
        constraintMap.put("intCol", singleValueSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain column name", conjuncts.get(0).contains("\"intCol\""));
        assertTrue("Conjunct should contain = operator", conjuncts.get(0).contains("="));
        assertTrue("Conjunct should contain value", conjuncts.get(0).contains("10"));
        assertEquals("Should have no parameters (embedded values)", 0, parameterValues.size());
    }

    @Test
    public void buildConjuncts_WithMultipleSingleValues_ReturnsInPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
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
        assertTrue("Conjunct should contain values", conjuncts.get(0).contains("10"));
        assertTrue("Conjunct should contain values", conjuncts.get(0).contains("20"));
        assertTrue("Conjunct should contain values", conjuncts.get(0).contains("30"));
    }

    @Test
    public void buildConjuncts_WithRangePredicate_ReturnsRangePredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.above(allocator, INT_TYPE, 10), Marker.below(allocator, INT_TYPE, 20)))
                .build();
        constraintMap.put("intCol", rangeSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain > operator", conjuncts.get(0).contains(">"));
        assertTrue("Conjunct should contain < operator", conjuncts.get(0).contains("<"));
        assertTrue("Conjunct should contain AND", conjuncts.get(0).contains("AND"));
    }

    @Test
    public void buildConjuncts_WithNullValueSet_ReturnsIsNullPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet nullSet = SortedRangeSet.newBuilder(INT_TYPE, true).build();
        constraintMap.put("intCol", nullSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain IS NULL", conjuncts.get(0).contains("IS NULL"));
    }

    @Test
    public void buildConjuncts_WithUnboundedRange_ReturnsIsNotNullPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet notNullSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.lowerUnbounded(allocator, INT_TYPE), Marker.upperUnbounded(allocator, INT_TYPE)))
                .build();
        constraintMap.put("intCol", notNullSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain IS NOT NULL", conjuncts.get(0).contains("IS NOT NULL"));
    }

    @Test
    public void buildConjuncts_WithNullAllowedRange_ReturnsOrPredicateWithIsNull()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeWithNull = SortedRangeSet.newBuilder(INT_TYPE, true)
                .add(new Range(Marker.above(allocator, INT_TYPE, 10), Marker.below(allocator, INT_TYPE, 20)))
                .build();
        constraintMap.put("intCol", rangeWithNull);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain OR", conjuncts.get(0).contains("OR"));
        assertTrue("Conjunct should contain IS NULL", conjuncts.get(0).contains("IS NULL"));
    }

    @Test
    public void buildConjuncts_WithPartitionColumn_FiltersOutPartitionColumn()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .build();
        constraintMap.put("intCol", rangeSet);
        constraintMap.put("partitionCol", rangeSet);

        fields.add(Field.nullable("intCol", INT_TYPE));
        fields.add(Field.nullable("partitionCol", INT_TYPE));
        
        Map<String, String> splitProperties = new HashMap<>();
        splitProperties.put("partitionCol", "p0");
        Split splitWithPartition = mock(Split.class);
        when(splitWithPartition.getProperties()).thenReturn(splitProperties);

        List<String> conjuncts = buildConjuncts(constraintMap, fields, splitWithPartition);

        assertFalse("Should have at least one conjunct", conjuncts.isEmpty());
        assertTrue("Conjunct should contain intCol", conjuncts.get(0).contains("\"intCol\""));
        assertFalse("Conjunct should not contain partitionCol", conjuncts.get(0).contains("\"partitionCol\""));
    }

    @Test
    public void buildConjuncts_WithNullSplit_DoesNotFilterPartitionColumns()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .build();
        constraintMap.put("intCol", rangeSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, null);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain intCol", conjuncts.get(0).contains("\"intCol\""));
    }

    @Test
    public void buildConjuncts_WithEmptyConstraints_ReturnsEmptyList()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertTrue("Should have no conjuncts", conjuncts.isEmpty());
    }

    @Test
    public void buildConjuncts_WithNullConstraints_ReturnsEmptyList()
    {
        fields.add(Field.nullable("intCol", INT_TYPE));
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), -1, Collections.emptyMap(), null);

        List<String> conjuncts = builder.buildConjuncts(fields, constraints, parameterValues, split);

        assertTrue("Should have no conjuncts", conjuncts.isEmpty());
    }

    @Test
    public void buildConjuncts_WithStringType_ReturnsStringPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet stringSet = SortedRangeSet.newBuilder(STRING_TYPE, false)
                .add(new Range(Marker.exactly(allocator, STRING_TYPE, "test"), Marker.exactly(allocator, STRING_TYPE, "test")))
                .build();
        constraintMap.put("stringCol", stringSet);

        fields.add(Field.nullable("stringCol", STRING_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain stringCol", conjuncts.get(0).contains("\"stringCol\""));
        assertTrue("Conjunct should contain quoted value", conjuncts.get(0).contains("'test'"));
    }

    @Test
    public void buildConjuncts_WithBooleanType_ReturnsBooleanPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet booleanSet = SortedRangeSet.newBuilder(BOOLEAN_TYPE, false)
                .add(new Range(Marker.exactly(allocator, BOOLEAN_TYPE, true), Marker.exactly(allocator, BOOLEAN_TYPE, true)))
                .build();
        constraintMap.put("boolCol", booleanSet);

        fields.add(Field.nullable("boolCol", BOOLEAN_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain boolCol", conjuncts.get(0).contains("\"boolCol\""));
        assertTrue("Conjunct should contain true", conjuncts.get(0).contains("true"));
    }

    @Test
    public void buildConjuncts_WithDecimalType_ReturnsDecimalPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet decimalSet = SortedRangeSet.newBuilder(DECIMAL_TYPE, false)
                .add(new Range(Marker.exactly(allocator, DECIMAL_TYPE, new BigDecimal("123.45")), 
                        Marker.exactly(allocator, DECIMAL_TYPE, new BigDecimal("123.45"))))
                .build();
        constraintMap.put("decimalCol", decimalSet);

        fields.add(Field.nullable("decimalCol", DECIMAL_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain decimalCol", conjuncts.get(0).contains("\"decimalCol\""));
    }

    @Test
    public void buildConjuncts_WithFloatType_ReturnsFloatPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet floatSet = SortedRangeSet.newBuilder(FLOAT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, FLOAT_TYPE, 123.45), 
                        Marker.exactly(allocator, FLOAT_TYPE, 123.45)))
                .build();
        constraintMap.put("floatCol", floatSet);

        fields.add(Field.nullable("floatCol", FLOAT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain floatCol", conjuncts.get(0).contains("\"floatCol\""));
    }

    @Test
    public void buildConjuncts_WithDateType_ReturnsDatePredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        long epochDays = java.time.LocalDate.of(2023, 1, 1).toEpochDay();
        ValueSet dateSet = SortedRangeSet.newBuilder(DATE_TYPE, false)
                .add(new Range(Marker.exactly(allocator, DATE_TYPE, epochDays), 
                        Marker.exactly(allocator, DATE_TYPE, epochDays)))
                .build();
        constraintMap.put("dateCol", dateSet);

        fields.add(Field.nullable("dateCol", DATE_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain dateCol", conjuncts.get(0).contains("\"dateCol\""));
        assertTrue("Conjunct should contain quoted date", conjuncts.get(0).contains("'"));
        assertTrue("Conjunct should contain formatted date", conjuncts.get(0).contains("2023-01-01"));
    }

    @Test
    public void buildConjuncts_WithRangeWithExactBounds_ReturnsCorrectPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 20)))
                .build();
        constraintMap.put("intCol", rangeSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain >=", conjuncts.get(0).contains(">="));
        assertTrue("Conjunct should contain <=", conjuncts.get(0).contains("<="));
    }

    @Test
    public void buildConjuncts_WithRangeWithAboveBound_ReturnsGreaterThanPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.above(allocator, INT_TYPE, 10), Marker.upperUnbounded(allocator, INT_TYPE)))
                .build();
        constraintMap.put("intCol", rangeSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain >", conjuncts.get(0).contains(">"));
        assertFalse("Conjunct should not contain <", conjuncts.get(0).contains("<"));
    }

    @Test
    public void buildConjuncts_WithRangeWithBelowBound_ReturnsLessThanPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.lowerUnbounded(allocator, INT_TYPE), Marker.below(allocator, INT_TYPE, 20)))
                .build();
        constraintMap.put("intCol", rangeSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain <", conjuncts.get(0).contains("<"));
        assertFalse("Conjunct should not contain >", conjuncts.get(0).contains(">"));
    }

    @Test
    public void buildConjuncts_WithDecimalFromNumber_ConvertsToBigDecimal()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet decimalSet = SortedRangeSet.newBuilder(DECIMAL_TYPE, false)
                .add(new Range(Marker.exactly(allocator, DECIMAL_TYPE, 123.45), 
                        Marker.exactly(allocator, DECIMAL_TYPE, 123.45)))
                .build();
        constraintMap.put("decimalCol", decimalSet);

        fields.add(Field.nullable("decimalCol", DECIMAL_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain decimalCol", conjuncts.get(0).contains("\"decimalCol\""));
    }

    @Test
    public void buildConjuncts_WithStringContainingQuotes_EscapesQuotes()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet stringSet = SortedRangeSet.newBuilder(STRING_TYPE, false)
                .add(new Range(Marker.exactly(allocator, STRING_TYPE, "test'value"), 
                        Marker.exactly(allocator, STRING_TYPE, "test'value")))
                .build();
        constraintMap.put("stringCol", stringSet);

        fields.add(Field.nullable("stringCol", STRING_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain escaped quotes", conjuncts.get(0).contains("''"));
    }

    @Test
    public void buildConjuncts_WithMultipleRanges_ReturnsOrPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet multiRangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .add(new Range(Marker.above(allocator, INT_TYPE, 20), Marker.below(allocator, INT_TYPE, 30)))
                .build();
        constraintMap.put("intCol", multiRangeSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain OR", conjuncts.get(0).contains("OR"));
    }

    // Note: Testing unsupported types requires creating ValueSets with those types,
    // which may not be possible with the Marker API. These error paths are defensive
    // and would only be hit if the code receives invalid data structures.

    @Test
    public void buildConjuncts_WithDecimalFromNumber_ConvertsCorrectly()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet decimalSet = SortedRangeSet.newBuilder(DECIMAL_TYPE, false)
                .add(new Range(Marker.exactly(allocator, DECIMAL_TYPE, new BigDecimal("123.45")), 
                        Marker.exactly(allocator, DECIMAL_TYPE, new BigDecimal("123.45"))))
                .build();
        constraintMap.put("decimalCol", decimalSet);

        fields.add(Field.nullable("decimalCol", DECIMAL_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);
        assertFalse("Should have conjuncts", conjuncts.isEmpty());
    }

    @Test
    public void buildConjuncts_WithRangeOnlyLowBound_ReturnsOnlyLowPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.upperUnbounded(allocator, INT_TYPE)))
                .build();
        constraintMap.put("intCol", rangeSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain >=", conjuncts.get(0).contains(">="));
        assertFalse("Conjunct should not contain <", conjuncts.get(0).contains("<"));
    }

    @Test
    public void buildConjuncts_WithRangeOnlyHighBound_ReturnsOnlyHighPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet rangeSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.lowerUnbounded(allocator, INT_TYPE), Marker.exactly(allocator, INT_TYPE, 20)))
                .build();
        constraintMap.put("intCol", rangeSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain <=", conjuncts.get(0).contains("<="));
        assertFalse("Conjunct should not contain >", conjuncts.get(0).contains(">"));
    }

    @Test
    public void buildConjuncts_WithMixedSingleValuesAndRanges_ReturnsCombinedPredicate()
    {
        Map<String, ValueSet> constraintMap = new LinkedHashMap<>();
        ValueSet mixedSet = SortedRangeSet.newBuilder(INT_TYPE, false)
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 10), Marker.exactly(allocator, INT_TYPE, 10)))
                .add(new Range(Marker.exactly(allocator, INT_TYPE, 20), Marker.exactly(allocator, INT_TYPE, 20)))
                .add(new Range(Marker.above(allocator, INT_TYPE, 30), Marker.below(allocator, INT_TYPE, 40)))
                .build();
        constraintMap.put("intCol", mixedSet);

        fields.add(Field.nullable("intCol", INT_TYPE));

        List<String> conjuncts = buildConjuncts(constraintMap, fields, split);

        assertEquals("Should have one conjunct", 1, conjuncts.size());
        assertTrue("Conjunct should contain IN", conjuncts.get(0).contains("IN"));
        assertTrue("Conjunct should contain OR", conjuncts.get(0).contains("OR"));
    }

    private List<String> buildConjuncts(Map<String, ValueSet> constraintMap, List<Field> fields, Split split)
    {
        Constraints constraints = new Constraints(constraintMap, Collections.emptyList(), Collections.emptyList(), -1, Collections.emptyMap(), null);
        return builder.buildConjuncts(fields, constraints, parameterValues, split);
    }
}
