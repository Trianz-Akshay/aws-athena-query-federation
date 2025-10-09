/*-
 * #%L
 * athena-neptune
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
package com.amazonaws.athena.connectors.neptune.propertygraph;

import com.amazonaws.athena.connector.lambda.domain.predicate.Marker.Bound;
import com.amazonaws.athena.connectors.neptune.Enums.SpecialKeys;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.junit.Test;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Simplified unit tests for complex expressions in Neptune Gremlin queries
 */
public class SimplifiedComplexExpressionTest
{
    @Mock
    private GraphTraversal<Element, Element> mockTraversal;
    
    @Before
    public void setUp()
    {
        MockitoAnnotations.openMocks(this);
        when(mockTraversal.has(anyString(), any(P.class))).thenReturn(mockTraversal);
        when(mockTraversal.where(any(GraphTraversal.class))).thenReturn(mockTraversal);
    }
    
    @Test
    public void testComplexExpressionWithMultipleConditions()
    {
        // Test complex expression: age > 25 AND salary < 100000
        GraphTraversal<Element, Element> result1 = GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
            "age",
            "25",
            new ArrowType.Int(32, true),
            Bound.ABOVE,
            GremlinQueryPreProcessor.Operator.GREATERTHAN
        );
        
        GraphTraversal<Element, Element> result2 = GremlinQueryPreProcessor.generateGremlinQueryPart(
            result1,
            "salary",
            "100000",
            new ArrowType.Int(32, true),
            Bound.BELOW,
            GremlinQueryPreProcessor.Operator.LESSTHAN
        );
        
        // Verify both conditions are applied
        verify(mockTraversal).has("age", P.gt(25));
        verify(mockTraversal).has("salary", P.lt(100000));
        Assert.assertEquals("Result should be the mock traversal", mockTraversal, result2);
    }
    
    @Test
    public void testComplexExpressionWithNestedConditions()
    {
        // Test complex expression with nested conditions: (age >= 18 AND age <= 65) OR (status = 'admin')
        GraphTraversal<Element, Element> ageLower = GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
            "age",
            "18",
            new ArrowType.Int(32, true),
            Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.GREATERTHAN
        );
        
        GraphTraversal<Element, Element> ageUpper = GremlinQueryPreProcessor.generateGremlinQueryPart(
            ageLower,
            "age",
            "65",
            new ArrowType.Int(32, true),
            Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.LESSTHAN
        );
        
        GraphTraversal<Element, Element> status = GremlinQueryPreProcessor.generateGremlinQueryPart(
            ageUpper,
            "status",
            "admin",
            ArrowType.Utf8.INSTANCE,
            Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.EQUALTO
        );
        
        // Verify all conditions are applied
        verify(mockTraversal).has("age", P.gte(18));
        verify(mockTraversal).has("age", P.lte(65));
        verify(mockTraversal).has("status", P.eq("admin"));
        Assert.assertEquals("Result should be the mock traversal", mockTraversal, status);
    }
    
    @Test
    public void testComplexExpressionWithSpecialKeys()
    {
        // Test complex expression with special keys: in > 100 AND out < 200
        GraphTraversal<Element, Element> inResult = GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
            SpecialKeys.IN.toString().toLowerCase(),
            "100",
            new ArrowType.Int(64, true),
            Bound.ABOVE,
            GremlinQueryPreProcessor.Operator.GREATERTHAN
        );
        
        GraphTraversal<Element, Element> outResult = GremlinQueryPreProcessor.generateGremlinQueryPart(
            inResult,
            SpecialKeys.OUT.toString().toLowerCase(),
            "200",
            new ArrowType.Int(64, true),
            Bound.BELOW,
            GremlinQueryPreProcessor.Operator.LESSTHAN
        );
        
        // Verify special key conditions are applied using where clauses (called twice: once for 'in', once for 'out')
        verify(mockTraversal, times(2)).where(any(GraphTraversal.class));
        Assert.assertEquals("Result should be the mock traversal", mockTraversal, outResult);
    }
    
    @Test
    public void testComplexExpressionWithMixedDataTypes()
    {
        // Test complex expression with mixed data types: name = 'John' AND age > 25 AND active = true
        GraphTraversal<Element, Element> nameResult = GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
            "name",
            "John",
            ArrowType.Utf8.INSTANCE,
            Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.EQUALTO
        );
        
        GraphTraversal<Element, Element> ageResult = GremlinQueryPreProcessor.generateGremlinQueryPart(
            nameResult,
            "age",
            "25",
            new ArrowType.Int(32, true),
            Bound.ABOVE,
            GremlinQueryPreProcessor.Operator.GREATERTHAN
        );
        
        GraphTraversal<Element, Element> activeResult = GremlinQueryPreProcessor.generateGremlinQueryPart(
            ageResult,
            "active",
            "true",
            ArrowType.Bool.INSTANCE,
            Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.EQUALTO
        );
        
        // Verify all data type conditions are applied
        verify(mockTraversal).has("name", P.eq("John"));
        verify(mockTraversal).has("age", P.gt(25));
        verify(mockTraversal).has("active", P.eq(true));
        Assert.assertEquals("Result should be the mock traversal", mockTraversal, activeResult);
    }
    
    @Test
    public void testComplexExpressionWithNotEqualConditions()
    {
        // Test complex expression with not equal conditions: status != 'inactive' AND role != 'guest'
        GraphTraversal<Element, Element> statusResult = GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
            "status",
            "inactive",
            ArrowType.Utf8.INSTANCE,
            Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.NOTEQUALTO
        );
        
        GraphTraversal<Element, Element> roleResult = GremlinQueryPreProcessor.generateGremlinQueryPart(
            statusResult,
            "role",
            "guest",
            ArrowType.Utf8.INSTANCE,
            Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.NOTEQUALTO
        );
        
        // Verify not equal conditions are applied
        verify(mockTraversal).has("status", P.neq("inactive"));
        verify(mockTraversal).has("role", P.neq("guest"));
        Assert.assertEquals("Result should be the mock traversal", mockTraversal, roleResult);
    }
    
    @Test
    public void testComplexExpressionWithLongValues()
    {
        // Test complex expression with long values: timestamp > 1609459200000 AND id != 0
        GraphTraversal<Element, Element> timestampResult = GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
            "timestamp",
            "1609459200000",
            new ArrowType.Int(64, true),
            Bound.ABOVE,
            GremlinQueryPreProcessor.Operator.GREATERTHAN
        );
        
        GraphTraversal<Element, Element> idResult = GremlinQueryPreProcessor.generateGremlinQueryPart(
            timestampResult,
            "id",
            "0",
            new ArrowType.Int(64, true),
            Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.NOTEQUALTO
        );
        
        // Verify long conditions are applied
        verify(mockTraversal).has("timestamp", P.gt(1609459200000L));
        verify(mockTraversal).where(any(GraphTraversal.class)); // id field uses where clause for special key
        Assert.assertEquals("Result should be the mock traversal", mockTraversal, idResult);
    }
    
    @Test
    public void testComplexExpressionWithBooleanLogic()
    {
        // Test complex expression with boolean logic: isActive = true AND isVerified = false
        GraphTraversal<Element, Element> activeResult = GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
            "isActive",
            "true",
            ArrowType.Bool.INSTANCE,
            Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.EQUALTO
        );
        
        GraphTraversal<Element, Element> verifiedResult = GremlinQueryPreProcessor.generateGremlinQueryPart(
            activeResult,
            "isVerified",
            "false",
            ArrowType.Bool.INSTANCE,
            Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.EQUALTO
        );
        
        // Verify boolean conditions are applied
        verify(mockTraversal).has("isActive", P.eq(true));
        verify(mockTraversal).has("isVerified", P.eq(false));
        Assert.assertEquals("Result should be the mock traversal", mockTraversal, verifiedResult);
    }
}
