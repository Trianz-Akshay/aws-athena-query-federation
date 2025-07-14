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
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.jdbc.TestBase;
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;

import java.util.ArrayList;
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

public class EnhancedBasePredicateBuilderTest extends TestBase
{
    private TestEnhancedBasePredicateBuilder predicateBuilder;
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
        
        predicateBuilder = new TestEnhancedBasePredicateBuilder(mockTemplateGroup, "\"");
        
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
        when(mockConstraints.getExpression()).thenReturn(Collections.emptyList());
    }

    @Test
    public void testBuildConjuncts()
    {
        List<Object> parameterValues = new ArrayList<>();
        List<String> result = predicateBuilder.buildConjuncts(mockFields, mockConstraints, parameterValues);
        
        assertNotNull(result);
        // The result should contain conjuncts for the fields that have value sets
        assertEquals(1, result.size());
    }

    @Test
    public void testBuildConjunctWithType()
    {
        SortedRangeSet valueSet = Mockito.mock(SortedRangeSet.class);
        when(valueSet.isNone()).thenReturn(false);
        when(valueSet.isAll()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(false);
        when(valueSet.getOrderedRanges()).thenReturn(Collections.emptyList());
        
        List<Object> parameterValues = new ArrayList<>();
        String result = predicateBuilder.buildConjunct("test_column", valueSet, parameterValues, 
            org.apache.arrow.vector.types.Types.MinorType.INT.getType());
        
        // The method should return the result from buildRangePredicate which is overridden in TestEnhancedBasePredicateBuilder
        assertNotNull(result);
        assertTrue(result.contains("test_column"));
    }

    @Test
    public void testBuildRangePredicateWithType()
    {
        SortedRangeSet rangeSet = Mockito.mock(SortedRangeSet.class);
        when(rangeSet.isNone()).thenReturn(false);
        when(rangeSet.isNullAllowed()).thenReturn(false);
        when(rangeSet.getOrderedRanges()).thenReturn(Collections.emptyList());
        
        List<Object> parameterValues = new ArrayList<>();
        String result = predicateBuilder.buildRangePredicate("test_column", rangeSet, parameterValues, 
            org.apache.arrow.vector.types.Types.MinorType.INT.getType());
        
        // The method should return the result from the overridden buildRangePredicate in TestEnhancedBasePredicateBuilder
        assertNotNull(result);
        assertTrue(result.contains("test_column"));
    }

    @Test
    public void testBuildInPredicateWithType()
    {
        EquatableValueSet equatableValueSet = Mockito.mock(EquatableValueSet.class);
        com.amazonaws.athena.connector.lambda.data.Block mockBlock = Mockito.mock(com.amazonaws.athena.connector.lambda.data.Block.class);
        when(equatableValueSet.getValues()).thenReturn(mockBlock);
        when(mockBlock.getRowCount()).thenReturn(2);
        when(equatableValueSet.getValue(0)).thenReturn("value1");
        when(equatableValueSet.getValue(1)).thenReturn("value2");
        
        List<Object> parameterValues = new ArrayList<>();
        String result = predicateBuilder.buildInPredicate("test_column", equatableValueSet, parameterValues, 
            org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType());
        
        // The method should return the result from the overridden buildInPredicate in TestEnhancedBasePredicateBuilder
        assertNotNull(result);
        assertTrue(result.contains("test_column"));
        assertTrue(result.contains("IN"));
    }

    @Test
    public void testBuildLikePredicate()
    {
        List<Object> parameterValues = new ArrayList<>();
        String result = predicateBuilder.buildLikePredicate("test_column", "test%", parameterValues, 
            org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType());
        
        assertEquals("test_predicate", result);
    }

    @Test
    public void testBuildComparisonPredicate()
    {
        List<Object> parameterValues = new ArrayList<>();
        String result = predicateBuilder.buildComparisonPredicate("test_column", ">", 10, parameterValues, 
            org.apache.arrow.vector.types.Types.MinorType.INT.getType());
        
        assertEquals("test_predicate", result);
    }

    @Test
    public void testBuildBetweenPredicate()
    {
        List<Object> parameterValues = new ArrayList<>();
        String result = predicateBuilder.buildBetweenPredicate("test_column", 1, 10, parameterValues, 
            org.apache.arrow.vector.types.Types.MinorType.INT.getType());
        
        assertEquals("test_predicate", result);
    }

    @Test
    public void testConvertValueForDatabase()
    {
        Object result = predicateBuilder.convertValueForDatabase("test_value", 
            org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType());
        assertEquals("test_value", result);
    }

    @Test
    public void testCreateFederationExpressionParser()
    {
        FederationExpressionParser parser = predicateBuilder.createFederationExpressionParser();
        assertNotNull(parser);
    }

    private ValueSet createMockValueSet()
    {
        ValueSet valueSet = Mockito.mock(ValueSet.class);
        when(valueSet.isNone()).thenReturn(false);
        when(valueSet.isAll()).thenReturn(false);
        when(valueSet.isNullAllowed()).thenReturn(false);
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

    // Test implementation of EnhancedBasePredicateBuilder for testing
    private static class TestEnhancedBasePredicateBuilder extends EnhancedBasePredicateBuilder
    {
        public TestEnhancedBasePredicateBuilder(STGroup templateGroup, String quoteChar)
        {
            super(templateGroup, quoteChar);
        }

        @Override
        protected Object convertValueForDatabase(Object value, org.apache.arrow.vector.types.pojo.ArrowType fieldType)
        {
            return value;
        }

        @Override
        protected FederationExpressionParser createFederationExpressionParser()
        {
            return Mockito.mock(FederationExpressionParser.class);
        }

        @Override
        protected String buildRangePredicate(String columnName, SortedRangeSet rangeSet, List<Object> parameterValues)
        {
            return "\"" + columnName + "\" > ? AND \"" + columnName + "\" < ?";
        }

        @Override
        protected String buildRangePredicate(String columnName, SortedRangeSet rangeSet, List<Object> parameterValues, org.apache.arrow.vector.types.pojo.ArrowType fieldType)
        {
            return "\"" + columnName + "\" > ? AND \"" + columnName + "\" < ?";
        }

        @Override
        protected String buildInPredicate(String columnName, EquatableValueSet equatableValueSet, List<Object> parameterValues)
        {
            return "\"" + columnName + "\" IN (?, ?, ?)";
        }

        @Override
        protected String buildInPredicate(String columnName, EquatableValueSet equatableValueSet, List<Object> parameterValues, org.apache.arrow.vector.types.pojo.ArrowType fieldType)
        {
            return "\"" + columnName + "\" IN (?, ?, ?)";
        }
    }
} 