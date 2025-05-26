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
import com.amazonaws.athena.connectors.neptune.Constants;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CustomSchemaRowWriterTest {

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
    void testBitField_WithBoolean() {
        Field field = new Field("boolField", FieldType.nullable(new ArrowType.Bool()), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        context.put("boolField", true);

        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("boolField"), any());
    }

    @Test
    void testBitField_WithArrayList() {
        Field field = new Field("boolField", FieldType.nullable(new ArrowType.Bool()), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> values = new ArrayList<>();
        values.add(true);
        context.put("boolField", values);

        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("boolField"), any());
    }

    @Test
    void testVarCharField_WithString() {
        Field field = new Field("stringField", FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        context.put("stringField", "test");

        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("stringField"), any());
    }

    @Test
    void testVarCharField_WithArrayList() {
        Field field = new Field("stringField", FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> values = new ArrayList<>();
        values.add("test1");
        values.add("test2");
        context.put("stringField", values);

        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("stringField"), any());
    }

    @Test
    void testVarCharField_WithOtherType() {
        Field field = new Field("stringField", FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        context.put("stringField", 42);

        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("stringField"), any());
    }

    @Test
    void testDateMilliField_WithDate() {
        Field field = new Field("dateField", FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        context.put("dateField", new Date());

        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("dateField"), any());
    }

    @Test
    void testDateMilliField_WithArrayList() {
        Field field = new Field("dateField", FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> values = new ArrayList<>();
        values.add(new Date());
        context.put("dateField", values);

        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("dateField"), any());
    }

    @Test
    void testIntField_WithInteger() {
        Field field = new Field("intField", FieldType.nullable(new ArrowType.Int(32, true)), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        context.put("intField", 42);

        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("intField"), any());
    }

    @Test
    void testIntField_WithArrayList() {
        Field field = new Field("intField", FieldType.nullable(new ArrowType.Int(32, true)), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> values = new ArrayList<>();
        values.add(42);
        context.put("intField", values);

        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("intField"), any());
    }

    @Test
    void testBigIntField_WithLong() {
        Field field = new Field("longField", FieldType.nullable(new ArrowType.Int(64, true)), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        context.put("longField", 1234567890L);

        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("longField"), any());
    }

    @Test
    void testBigIntField_WithArrayList() {
        Field field = new Field("longField", FieldType.nullable(new ArrowType.Int(64, true)), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> values = new ArrayList<>();
        values.add(1234567890L);
        context.put("longField", values);

        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("longField"), any());
    }

    @Test
    void testFloat4Field_WithFloat() {
        Field field = new Field("floatField", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        context.put("floatField", 3.14f);

        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("floatField"), any());
    }

    @Test
    void testFloat4Field_WithArrayList() {
        Field field = new Field("floatField", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> values = new ArrayList<>();
        values.add(3.14f);
        context.put("floatField", values);

        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("floatField"), any());
    }

    @Test
    void testFloat8Field_WithDouble() {
        Field field = new Field("doubleField", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        context.put("doubleField", 2.718);

        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("doubleField"), any());
    }

    @Test
    void testFloat8Field_WithArrayList() {
        Field field = new Field("doubleField", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        ArrayList<Object> values = new ArrayList<>();
        values.add(2.718);
        context.put("doubleField", values);

        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("doubleField"), any());
    }

    @Test
    void testIdField() {
        Field field = new Field("id", FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        context.put(T.id.toString(), "vertex-123");

        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("id"), any());
    }

    @Test
    void testNullValues() {
        Field field = new Field("nullField", FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        context.put("nullField", null);

        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("nullField"), any());
    }

    @Test
    void testEmptyValues() {
        Field field = new Field("emptyField", FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        context.put("emptyField", "");

        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("emptyField"), any());
    }

    @Test
    void testCaseInsensitive_Disabled() {
        configOptions.put(Constants.SCHEMA_CASE_INSEN, "false");
        Field field = new Field("MixedCaseField", FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        Map<String, Object> context = new HashMap<>();
        context.put("MixedCaseField", "test");

        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq("MixedCaseField"), any());
    }
} 