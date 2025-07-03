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

    @Mock
    private GraphTraversal<Element, Element> mockTraversal;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(mockTraversal.has(anyString(), any(P.class))).thenReturn(mockTraversal);
        when(mockTraversal.where(any(GraphTraversal.class))).thenReturn(mockTraversal);
    }

    @Test
    public void testBooleanEqualTo() {
        when(mockTraversal.has("boolField", P.eq(true))).thenReturn(mockTraversal);
        
        GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
            "boolField",
            "true",
            new ArrowType.Bool(),
            Marker.Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.EQUALTO
        );
        
        verify(mockTraversal).has("boolField", P.eq(true));
    }

    @Test
    public void testIntegerGreaterThan() {
        when(mockTraversal.has("intField", P.gt(42))).thenReturn(mockTraversal);
        
        GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
            "intField",
            "42",
            new ArrowType.Int(32, true),
            Marker.Bound.ABOVE,
            GremlinQueryPreProcessor.Operator.GREATERTHAN
        );
        
        verify(mockTraversal).has("intField", P.gt(42));
    }

    @Test
    public void testLongLessThan() {
        when(mockTraversal.has("longField", P.lt(1000L))).thenReturn(mockTraversal);
        
        GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
            "longField",
            "1000",
            new ArrowType.Int(64, true),
            Marker.Bound.ABOVE,
            GremlinQueryPreProcessor.Operator.LESSTHAN
        );
        
        verify(mockTraversal).has("longField", P.lt(1000L));
    }

    @Test
    public void testFloatGreaterThanEqual() {
        when(mockTraversal.has("floatField", P.gte(3.14f))).thenReturn(mockTraversal);
        
        GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
            "floatField",
            "3.14",
            new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
            Marker.Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.GREATERTHAN
        );
        
        verify(mockTraversal).has("floatField", P.gte(3.14f));
    }

    @Test
    public void testDoubleLessThanEqual() {
        when(mockTraversal.has("doubleField", P.lte(2.718))).thenReturn(mockTraversal);
        
        GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
            "doubleField",
            "2.718",
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
            Marker.Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.LESSTHAN
        );
        
        verify(mockTraversal).has("doubleField", P.lte(2.718));
    }

    @Test
    public void testStringNotEqual() {
        when(mockTraversal.has("stringField", P.neq("test"))).thenReturn(mockTraversal);
        
        GremlinQueryPreProcessor.generateGremlinQueryPart(
            mockTraversal,
            "stringField",
            "test",
            new ArrowType.Utf8(),
            Marker.Bound.EXACTLY,
            GremlinQueryPreProcessor.Operator.NOTEQUALTO
        );
        
        verify(mockTraversal).has("stringField", P.neq("test"));
    }

    @Test
    public void testInVertexIdGreaterThan() {
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
    public void testOutVertexIdLessThan() {
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
    public void testElementIdEqual() {
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
    public void testInVertexIdGreaterThanEqual() {
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
    public void testOutVertexIdLessThanEqual() {
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
    public void testElementIdNotEqual() {
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