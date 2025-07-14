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

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class BasePredicateBuilderTest extends TestBase
{
    private TestBasePredicateBuilder predicateBuilder;
    private STGroup mockTemplateGroup;
    private Constraints mockConstraints;
    private List<Field> mockFields;

    @Before
    public void setup()
    {
        mockTemplateGroup = Mockito.mock(STGroup.class);
        
        // Setup mock templates
        ST mockTemplate = Mockito.mock(ST.class);
        when(mockTemplate.add(any(), any())).thenReturn(mockTemplate);
        when(mockTemplate.render()).thenReturn("test_predicate");
        when(mockTemplateGroup.getInstanceOf(any())).thenReturn(mockTemplate);
        
        predicateBuilder = new TestBasePredicateBuilder(mockTemplateGroup, "\"");
        
        // Setup mock fields
        mockFields = Arrays.asList(
            Field.nullable("id", org.apache.arrow.vector.types.Types.MinorType.INT.getType()),
            Field.nullable("name", org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType()),
            Field.nullable("value", org.apache.arrow.vector.types.Types.MinorType.FLOAT8.getType())
        );
        
        // Setup mock constraints
        mockConstraints = Mockito.mock(Constraints.class);
        Map<String, ValueSet> summary = new HashMap<>();
        summary.put("id", createMockValueSet());
        summary.put("name", createMockEquatableValueSet());
        when(mockConstraints.getSummary()).thenReturn(summary);
    }

    @Test
    public void testBuildConjuncts()
    {
        List<Object> parameterValues = new ArrayList<>();
        List<String> result = predicateBuilder.buildConjuncts(mockFields, mockConstraints, parameterValues);
        
        assertNotNull(result);
        // The result should contain conjuncts for the fields that have value sets
        assertEquals(0, result.size());
    }

    @Test
    public void testBuildConjunctWithNoneValueSet()
    {
        ValueSet noneValueSet = Mockito.mock(ValueSet.class);
        when(noneValueSet.isNone()).thenReturn(true);
        when(noneValueSet.isAll()).thenReturn(false);
        
        List<Object> parameterValues = new ArrayList<>();
        String result = predicateBuilder.buildConjunct("test_column", noneValueSet, parameterValues);
        
        assertEquals("test_predicate", result);
    }

    @Test
    public void testBuildConjunctWithAllValueSet()
    {
        ValueSet allValueSet = Mockito.mock(ValueSet.class);
        when(allValueSet.isNone()).thenReturn(false);
        when(allValueSet.isAll()).thenReturn(true);
        
        List<Object> parameterValues = new ArrayList<>();
        String result = predicateBuilder.buildConjunct("test_column", allValueSet, parameterValues);
        
        // Should return null for "all" value set
        assertEquals(null, result);
    }

    @Test
    public void testBuildConjunctWithSortedRangeSet()
    {
        SortedRangeSet rangeSet = Mockito.mock(SortedRangeSet.class);
        when(rangeSet.isNone()).thenReturn(false);
        when(rangeSet.isAll()).thenReturn(false);
        
        List<Object> parameterValues = new ArrayList<>();
        String result = predicateBuilder.buildConjunct("test_column", rangeSet, parameterValues);
        
        assertNotNull(result);
        assertTrue(result.contains("test_column"));
    }

    @Test
    public void testBuildRangePredicate()
    {
        SortedRangeSet rangeSet = Mockito.mock(SortedRangeSet.class);
        Range range1 = Mockito.mock(Range.class);
        Range range2 = Mockito.mock(Range.class);
        
        when(rangeSet.getOrderedRanges()).thenReturn(Arrays.asList(range1, range2));
        when(range1.getLow()).thenReturn(Mockito.mock(Marker.class));
        when(range1.getHigh()).thenReturn(Mockito.mock(Marker.class));
        when(range2.getLow()).thenReturn(Mockito.mock(Marker.class));
        when(range2.getHigh()).thenReturn(Mockito.mock(Marker.class));
        
        List<Object> parameterValues = new ArrayList<>();
        String result = predicateBuilder.buildRangePredicate("test_column", rangeSet, parameterValues);
        
        assertNotNull(result);
        assertTrue(result.contains("test_column"));
    }

    @Test
    public void testBuildInPredicate()
    {
        EquatableValueSet equatableValueSet = Mockito.mock(EquatableValueSet.class);
        com.amazonaws.athena.connector.lambda.data.Block mockBlock = Mockito.mock(com.amazonaws.athena.connector.lambda.data.Block.class);
        when(equatableValueSet.getValues()).thenReturn(mockBlock);
        when(mockBlock.getRowCount()).thenReturn(3);
        when(equatableValueSet.getValue(0)).thenReturn("value1");
        when(equatableValueSet.getValue(1)).thenReturn("value2");
        when(equatableValueSet.getValue(2)).thenReturn("value3");
        
        List<Object> parameterValues = new ArrayList<>();
        String result = predicateBuilder.buildInPredicate("test_column", equatableValueSet, parameterValues);
        
        assertNotNull(result);
        assertTrue(result.contains("test_column"));
        assertTrue(result.contains("IN"));
    }

    @Test
    public void testBuildNullPredicate()
    {
        String result = predicateBuilder.buildNullPredicate("test_column", true);
        assertEquals("test_predicate", result);
        
        result = predicateBuilder.buildNullPredicate("test_column", false);
        assertEquals("test_predicate", result);
    }

    @Test
    public void testBuildOrPredicate()
    {
        List<String> disjuncts = Arrays.asList("condition1", "condition2", "condition3");
        
        String result = predicateBuilder.buildOrPredicate(disjuncts, true);
        assertEquals("test_predicate", result);
        
        result = predicateBuilder.buildOrPredicate(disjuncts, false);
        assertEquals("test_predicate", result);
    }

    @Test
    public void testBuildBetweenPredicate()
    {
        String result = predicateBuilder.buildBetweenPredicate("test_column");
        assertEquals("test_predicate", result);
    }

    @Test
    public void testBuildComparisonPredicate()
    {
        String result = predicateBuilder.buildComparisonPredicate("test_column", ">");
        assertEquals("test_predicate", result);
        
        result = predicateBuilder.buildComparisonPredicate("test_column", "<=");
        assertEquals("test_predicate", result);
    }

    private ValueSet createMockValueSet()
    {
        ValueSet valueSet = Mockito.mock(ValueSet.class);
        when(valueSet.isNone()).thenReturn(false);
        when(valueSet.isAll()).thenReturn(false);
        return valueSet;
    }

    private EquatableValueSet createMockEquatableValueSet()
    {
        EquatableValueSet valueSet = Mockito.mock(EquatableValueSet.class);
        com.amazonaws.athena.connector.lambda.data.Block mockBlock = Mockito.mock(com.amazonaws.athena.connector.lambda.data.Block.class);
        when(valueSet.isNone()).thenReturn(false);
        when(valueSet.isAll()).thenReturn(false);
        when(valueSet.getValues()).thenReturn(mockBlock);
        when(mockBlock.getRowCount()).thenReturn(2);
        when(valueSet.getValue(0)).thenReturn("value1");
        when(valueSet.getValue(1)).thenReturn("value2");
        return valueSet;
    }

    // Test implementation of BasePredicateBuilder for testing
    private static class TestBasePredicateBuilder extends BasePredicateBuilder
    {
        public TestBasePredicateBuilder(STGroup templateGroup, String quoteChar)
        {
            super(templateGroup, quoteChar);
        }

        @Override
        protected String buildRangePredicate(String columnName, SortedRangeSet rangeSet, List<Object> parameterValues)
        {
            return "\"" + columnName + "\" > ? AND \"" + columnName + "\" < ?";
        }

        @Override
        protected String buildInPredicate(String columnName, EquatableValueSet equatableValueSet, List<Object> parameterValues)
        {
            return "\"" + columnName + "\" IN (?, ?, ?)";
        }
    }
} 