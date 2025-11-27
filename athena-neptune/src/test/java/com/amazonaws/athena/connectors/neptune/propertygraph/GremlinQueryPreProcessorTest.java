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

import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connectors.neptune.Enums.SpecialKeys;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GremlinQueryPreProcessorTest {

    public static final String BOOL_FIELD = "boolField";
    public static final String INT_FIELD = "intField";
    public static final String LONG_FIELD = "longField";
    public static final String FLOAT_FIELD = "floatField";
    public static final String DOUBLE_FIELD = "doubleField";
    public static final String STRING_FIELD = "stringField";
    public static final String TEST_VALUE = "test";
    @Mock
    private GraphTraversal<Element, Element> mockTraversal;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(mockTraversal.has(anyString(), any(P.class))).thenReturn(mockTraversal);
        when(mockTraversal.where(any(GraphTraversal.class))).thenReturn(mockTraversal);
    }

    @Test
    public void generateGremlinQueryPart_WithBooleanFieldAndEqualToOperator_AppliesCorrectPredicate() {
        when(mockTraversal.has(BOOL_FIELD, P.eq(true))).thenReturn(mockTraversal);

        GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
                BOOL_FIELD,
            "true",
            new ArrowType.Bool(),
            Marker.Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.EQUALTO
        );

        verify(mockTraversal).has(BOOL_FIELD, P.eq(true));
    }

    @Test
    public void generateGremlinQueryPart_WithIntegerFieldAndGreaterThanOperator_AppliesCorrectPredicate() {
        when(mockTraversal.has(INT_FIELD, P.gt(42))).thenReturn(mockTraversal);

        GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
                INT_FIELD,
            "42",
            new ArrowType.Int(32, true),
            Marker.Bound.ABOVE,
            GremlinQueryPreProcessor.Operator.GREATERTHAN
        );

        verify(mockTraversal).has(INT_FIELD, P.gt(42));
    }

    @Test
    public void generateGremlinQueryPart_WithLongFieldAndLessThanOperator_AppliesCorrectPredicate() {
        when(mockTraversal.has(LONG_FIELD, P.lt(1000L))).thenReturn(mockTraversal);

        GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
                LONG_FIELD,
            "1000",
            new ArrowType.Int(64, true),
            Marker.Bound.ABOVE,
            GremlinQueryPreProcessor.Operator.LESSTHAN
        );

        verify(mockTraversal).has(LONG_FIELD, P.lt(1000L));
    }

    @Test
    public void generateGremlinQueryPart_WithFloatFieldAndGreaterThanEqualOperator_AppliesCorrectPredicate() {
        when(mockTraversal.has(FLOAT_FIELD, P.gte(3.14f))).thenReturn(mockTraversal);

        GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
                FLOAT_FIELD,
            "3.14",
            new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
            Marker.Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.GREATERTHAN
        );

        verify(mockTraversal).has(FLOAT_FIELD, P.gte(3.14f));
    }

    @Test
    public void generateGremlinQueryPart_WithDoubleFieldAndLessThanEqualOperator_AppliesCorrectPredicate() {
        when(mockTraversal.has(DOUBLE_FIELD, P.lte(2.718))).thenReturn(mockTraversal);

        GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
                DOUBLE_FIELD,
            "2.718",
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
            Marker.Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.LESSTHAN
        );

        verify(mockTraversal).has(DOUBLE_FIELD, P.lte(2.718));
    }

    @Test
    public void generateGremlinQueryPart_WithStringFieldAndNotEqualOperator_AppliesCorrectPredicate() {
        when(mockTraversal.has(STRING_FIELD, P.neq(TEST_VALUE))).thenReturn(mockTraversal);

        GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
                STRING_FIELD,
            TEST_VALUE,
            new ArrowType.Utf8(),
            Marker.Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.NOTEQUALTO
        );

        verify(mockTraversal).has(STRING_FIELD, P.neq(TEST_VALUE));
    }

    @Test
    public void generateGremlinQueryPart_WithInVertexIdAndGreaterThanOperator_UsesWhereClause() {
        GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
            SpecialKeys.IN.toString().toLowerCase(),
            "123",
            new ArrowType.Int(64, true),
            Marker.Bound.ABOVE,
            GremlinQueryPreProcessor.Operator.GREATERTHAN
        );

        verify(mockTraversal).where(any(GraphTraversal.class));
    }

    @Test
    public void generateGremlinQueryPart_WithOutVertexIdAndLessThanOperator_UsesWhereClause() {
        GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
            SpecialKeys.OUT.toString().toLowerCase(),
            "456",
            new ArrowType.Int(64, true),
            Marker.Bound.ABOVE,
            GremlinQueryPreProcessor.Operator.LESSTHAN
        );

        verify(mockTraversal).where(any(GraphTraversal.class));
    }

    @Test
    public void generateGremlinQueryPart_WithElementIdAndEqualOperator_UsesWhereClause() {
        GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
            SpecialKeys.ID.toString().toLowerCase(),
            "789",
            new ArrowType.Int(64, true),
            Marker.Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.EQUALTO
        );

        verify(mockTraversal).where(any(GraphTraversal.class));
    }

    @Test
    public void generateGremlinQueryPart_WithInVertexIdAndGreaterThanEqualOperator_UsesWhereClause() {
        GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
            SpecialKeys.IN.toString().toLowerCase(),
            "123",
            new ArrowType.Int(64, true),
            Marker.Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.GREATERTHAN
        );

        verify(mockTraversal).where(any(GraphTraversal.class));
    }

    @Test
    public void generateGremlinQueryPart_WithOutVertexIdAndLessThanEqualOperator_UsesWhereClause() {
        GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
            SpecialKeys.OUT.toString().toLowerCase(),
            "456",
            new ArrowType.Int(64, true),
            Marker.Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.LESSTHAN
        );

        verify(mockTraversal).where(any(GraphTraversal.class));
    }

    @Test
    public void generateGremlinQueryPart_WithElementIdAndNotEqualOperator_UsesWhereClause() {
        GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
            SpecialKeys.ID.toString().toLowerCase(),
            "789",
            new ArrowType.Int(64, true),
            Marker.Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.NOTEQUALTO
        );

        verify(mockTraversal).where(any(GraphTraversal.class));
    }
} 