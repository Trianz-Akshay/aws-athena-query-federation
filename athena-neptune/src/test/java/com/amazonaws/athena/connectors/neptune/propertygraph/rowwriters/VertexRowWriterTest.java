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
package com.amazonaws.athena.connectors.neptune.propertygraph.rowwriters;

import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connectors.neptune.Constants;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.apache.arrow.vector.types.DateUnit;

import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class VertexRowWriterTest {

    @Mock
    private GeneratedRowWriter.RowWriterBuilder mockBuilder;

    private Map<String, String> configOptions;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        configOptions = new HashMap<>();
        configOptions.put(Constants.SCHEMA_CASE_INSEN, "true");
        when(mockBuilder.withExtractor(anyString(), any())).thenReturn(mockBuilder);
    }

    @Test
    void testBitField() {
        Field field = new Field("boolField", FieldType.nullable(new ArrowType.Bool()), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> values = new ArrayList<>();
        values.add(true);
        context.put("boolField", values);

        VertexRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("boolField"), any());
    }

    @Test
    void testVarCharField() {
        Field field = new Field("stringField", FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> values = new ArrayList<>();
        values.add("test");
        context.put("stringField", values);

        VertexRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("stringField"), any());
    }

    @Test
    void testDateMilliField() {
        Field field = new Field("dateField", FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> values = new ArrayList<>();
        values.add(new Date());
        context.put("dateField", values);

        VertexRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("dateField"), any());
    }

    @Test
    void testIntField() {
        Field field = new Field("intField", FieldType.nullable(new ArrowType.Int(32, true)), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> values = new ArrayList<>();
        values.add(42);
        context.put("intField", values);

        VertexRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("intField"), any());
    }

    @Test
    void testBigIntField() {
        Field field = new Field("longField", FieldType.nullable(new ArrowType.Int(64, true)), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> values = new ArrayList<>();
        values.add(1234567890L);
        context.put("longField", values);

        VertexRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("longField"), any());
    }

    @Test
    void testFloat4Field() {
        Field field = new Field("floatField", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> values = new ArrayList<>();
        values.add(3.14f);
        context.put("floatField", values);

        VertexRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("floatField"), any());
    }

    @Test
    void testFloat8Field() {
        Field field = new Field("doubleField", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> values = new ArrayList<>();
        values.add(2.718);
        context.put("doubleField", values);

        VertexRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("doubleField"), any());
    }

    @Test
    void testIdField() {
        Field field = new Field("id", FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        context.put(T.id.toString(), "vertex-123");

        VertexRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("id"), any());
    }

    @Test
    void testMultiValueVarCharField() {
        Field field = new Field("tags", FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> values = new ArrayList<>();
        values.add("tag1");
        values.add("tag2");
        values.add("tag3");
        context.put("tags", values);

        VertexRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("tags"), any());
    }

    @Test
    void testNullValues() {
        Field field = new Field("nullField", FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> values = new ArrayList<>();
        values.add(null);
        context.put("nullField", values);

        VertexRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("nullField"), any());
    }

    @Test
    void testEmptyValues() {
        Field field = new Field("emptyField", FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> values = new ArrayList<>();
        values.add("");
        context.put("emptyField", values);

        VertexRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("emptyField"), any());
    }

    @Test
    void testCaseInsensitiveDisabled() {
        configOptions.put(Constants.SCHEMA_CASE_INSEN, "false");
        Field field = new Field("MixedCaseField", FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> values = new ArrayList<>();
        values.add("test");
        context.put("MixedCaseField", values);

        VertexRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("MixedCaseField"), any());
    }
} 