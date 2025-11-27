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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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
    public static final String AGE = "age";
    public static final String SALARY = "salary";
    public static final String STATUS = "status";
    public static final String NAME = "name";
    public static final String ACTIVE = "active";
    public static final String ROLE = "role";
    public static final String TIMESTAMP = "timestamp";
    public static final String ID = "id";
    public static final String IS_ACTIVE = "isActive";
    public static final String IS_VERIFIED = "isVerified";
    public static final String ADMIN_VALUE = "admin";
    public static final String JOHN_VALUE = "John";
    public static final String INACTIVE_VALUE = "inactive";
    public static final String GUEST_VALUE = "guest";

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
    public void generateGremlinQueryPart_WithMultipleConditions_AppliesAllConditions()
    {
        // Test complex expression: age > 25 AND salary < 100000
        GraphTraversal<Element, Element> result1 = GremlinQueryPreProcessor.generateGremlinQueryPart(
                mockTraversal,
                AGE,
                "25",
                new ArrowType.Int(32, true),
                Bound.ABOVE,
                GremlinQueryPreProcessor.Operator.GREATERTHAN
        );

        GraphTraversal<Element, Element> result2 = GremlinQueryPreProcessor.generateGremlinQueryPart(
                result1,
                SALARY,
                "100000",
                new ArrowType.Int(32, true),
                Bound.BELOW,
                GremlinQueryPreProcessor.Operator.LESSTHAN
        );

        // Verify both conditions are applied
        verify(mockTraversal).has(AGE, P.gt(25));
        verify(mockTraversal).has(SALARY, P.lt(100000));
        Assert.assertEquals("Result should be the mock traversal", mockTraversal, result2);
    }

    @Test
    public void generateGremlinQueryPart_WithNestedConditions_AppliesAllNestedConditions()
    {
        // Test complex expression with nested conditions: (age >= 18 AND age <= 65) OR (status = 'admin')
        GraphTraversal<Element, Element> ageLower = GremlinQueryPreProcessor.generateGremlinQueryPart(
                mockTraversal,
                AGE,
                "18",
                new ArrowType.Int(32, true),
                Bound.EXACTLY,
                GremlinQueryPreProcessor.Operator.GREATERTHAN
        );

        GraphTraversal<Element, Element> ageUpper = GremlinQueryPreProcessor.generateGremlinQueryPart(
                ageLower,
                AGE,
                "65",
                new ArrowType.Int(32, true),
                Bound.EXACTLY,
                GremlinQueryPreProcessor.Operator.LESSTHAN
        );

        GraphTraversal<Element, Element> status = GremlinQueryPreProcessor.generateGremlinQueryPart(
                ageUpper,
                STATUS,
                ADMIN_VALUE,
                ArrowType.Utf8.INSTANCE,
                Bound.EXACTLY,
                GremlinQueryPreProcessor.Operator.EQUALTO
        );

        // Verify all conditions are applied
        verify(mockTraversal).has(AGE, P.gte(18));
        verify(mockTraversal).has(AGE, P.lte(65));
        verify(mockTraversal).has(STATUS, P.eq(ADMIN_VALUE));
        Assert.assertEquals("Result should be the mock traversal", mockTraversal, status);
    }

    @Test
    public void generateGremlinQueryPart_WithSpecialKeys_UsesWhereClauses()
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
    public void generateGremlinQueryPart_WithMixedDataTypes_AppliesCorrectPredicates()
    {
        // Test complex expression with mixed data types: name = 'John' AND age > 25 AND active = true
        GraphTraversal<Element, Element> nameResult = GremlinQueryPreProcessor.generateGremlinQueryPart(
                mockTraversal,
                NAME,
                JOHN_VALUE,
                ArrowType.Utf8.INSTANCE,
                Bound.EXACTLY,
                GremlinQueryPreProcessor.Operator.EQUALTO
        );

        GraphTraversal<Element, Element> ageResult = GremlinQueryPreProcessor.generateGremlinQueryPart(
                nameResult,
                AGE,
                "25",
                new ArrowType.Int(32, true),
                Bound.ABOVE,
                GremlinQueryPreProcessor.Operator.GREATERTHAN
        );

        GraphTraversal<Element, Element> activeResult = GremlinQueryPreProcessor.generateGremlinQueryPart(
                ageResult,
                ACTIVE,
                "true",
                ArrowType.Bool.INSTANCE,
                Bound.EXACTLY,
                GremlinQueryPreProcessor.Operator.EQUALTO
        );

        // Verify all data type conditions are applied
        verify(mockTraversal).has(NAME, P.eq(JOHN_VALUE));
        verify(mockTraversal).has(AGE, P.gt(25));
        verify(mockTraversal).has(ACTIVE, P.eq(true));
        Assert.assertEquals("Result should be the mock traversal", mockTraversal, activeResult);
    }

    @Test
    public void generateGremlinQueryPart_WithNotEqualConditions_AppliesNotEqualPredicates()
    {
        // Test complex expression with not equal conditions: status != 'inactive' AND role != 'guest'
        GraphTraversal<Element, Element> statusResult = GremlinQueryPreProcessor.generateGremlinQueryPart(
                mockTraversal,
                STATUS,
                INACTIVE_VALUE,
                ArrowType.Utf8.INSTANCE,
                Bound.EXACTLY,
                GremlinQueryPreProcessor.Operator.NOTEQUALTO
        );

        GraphTraversal<Element, Element> roleResult = GremlinQueryPreProcessor.generateGremlinQueryPart(
                statusResult,
                ROLE,
                GUEST_VALUE,
                ArrowType.Utf8.INSTANCE,
                Bound.EXACTLY,
                GremlinQueryPreProcessor.Operator.NOTEQUALTO
        );

        // Verify not equal conditions are applied
        verify(mockTraversal).has(STATUS, P.neq(INACTIVE_VALUE));
        verify(mockTraversal).has(ROLE, P.neq(GUEST_VALUE));
        Assert.assertEquals("Result should be the mock traversal", mockTraversal, roleResult);
    }

    @Test
    public void generateGremlinQueryPart_WithLongValues_AppliesLongPredicates()
    {
        // Test complex expression with long values: timestamp > 1609459200000 AND id != 0
        GraphTraversal<Element, Element> timestampResult = GremlinQueryPreProcessor.generateGremlinQueryPart(
                mockTraversal,
                TIMESTAMP,
                "1609459200000",
                new ArrowType.Int(64, true),
                Bound.ABOVE,
                GremlinQueryPreProcessor.Operator.GREATERTHAN
        );

        GraphTraversal<Element, Element> idResult = GremlinQueryPreProcessor.generateGremlinQueryPart(
                timestampResult,
                ID,
                "0",
                new ArrowType.Int(64, true),
                Bound.EXACTLY,
                GremlinQueryPreProcessor.Operator.NOTEQUALTO
        );

        // Verify long conditions are applied
        verify(mockTraversal).has(TIMESTAMP, P.gt(1609459200000L));
        verify(mockTraversal).where(any(GraphTraversal.class)); // id field uses where clause for special key
        Assert.assertEquals("Result should be the mock traversal", mockTraversal, idResult);
    }

    @Test
    public void generateGremlinQueryPart_WithBooleanLogic_AppliesBooleanPredicates()
    {
        // Test complex expression with boolean logic: isActive = true AND isVerified = false
        GraphTraversal<Element, Element> activeResult = GremlinQueryPreProcessor.generateGremlinQueryPart(
                mockTraversal,
                IS_ACTIVE,
                "true",
                ArrowType.Bool.INSTANCE,
                Bound.EXACTLY,
                GremlinQueryPreProcessor.Operator.EQUALTO
        );

        GraphTraversal<Element, Element> verifiedResult = GremlinQueryPreProcessor.generateGremlinQueryPart(
                activeResult,
                IS_VERIFIED,
                "false",
                ArrowType.Bool.INSTANCE,
                Bound.EXACTLY,
                GremlinQueryPreProcessor.Operator.EQUALTO
        );

        // Verify boolean conditions are applied
        verify(mockTraversal).has(IS_ACTIVE, P.eq(true));
        verify(mockTraversal).has(IS_VERIFIED, P.eq(false));
        Assert.assertEquals("Result should be the mock traversal", mockTraversal, verifiedResult);
    }
}
