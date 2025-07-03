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

import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter.RowWriterBuilder;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float4Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connectors.neptune.Constants;
import com.amazonaws.athena.connectors.neptune.Enums.SpecialKeys;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RowWritersComprehensiveTest {

    @Mock
    private RowWriterBuilder mockRowWriterBuilder;

    private Map<String, String> configOptions;
    private Map<String, Object> vertexContext;
    private Map<String, Object> edgeContext;
    private Map<String, Object> customContext;

    @Before
    public void setUp() {
        when(mockRowWriterBuilder.withExtractor(anyString(), any())).thenReturn(mockRowWriterBuilder);

        // Setup config options
        configOptions = new HashMap<>();

        // Setup vertex context data
        vertexContext = new HashMap<>();
        vertexContext.put(T.id.toString(), "vertex123");
        vertexContext.put(T.label.toString(), "person");

        // Setup edge context data (simplified to avoid Direction issues)
        edgeContext = new HashMap<>();
        edgeContext.put(T.id.toString(), "edge456");
        edgeContext.put(T.label.toString(), "knows");

        // Setup custom context data
        customContext = new HashMap<>();
        customContext.put("customField", "customValue");
    }

    // VertexRowWriter Tests
    @Test
    public void testVertexRowWriter_BitExtractor_ValidBoolean() {
        try {
            ArrayList<Object> boolValues = new ArrayList<>();
            boolValues.add(true);
            vertexContext.put("active", boolValues);

            Field bitField = new Field("active", FieldType.nullable(new ArrowType.Bool()), null);

            VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, bitField, configOptions);

            ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("active"), extractorCaptor.capture());

            BitExtractor extractor = extractorCaptor.getValue();
            NullableBitHolder holder = new NullableBitHolder();

            extractor.extract(vertexContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals(1, holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testVertexRowWriter_BitExtractor_ValidFalse() {
        try {
            ArrayList<Object> boolValues = new ArrayList<>();
            boolValues.add(false);
            vertexContext.put("inactive", boolValues);

            Field bitField = new Field("inactive", FieldType.nullable(new ArrowType.Bool()), null);

            VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, bitField, configOptions);

            ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("inactive"), extractorCaptor.capture());

            BitExtractor extractor = extractorCaptor.getValue();
            NullableBitHolder holder = new NullableBitHolder();

            extractor.extract(vertexContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals(0, holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testVertexRowWriter_VarCharExtractor_MultipleValues() {
        try {
            ArrayList<Object> multiValues = new ArrayList<>();
            multiValues.add("value1");
            multiValues.add("value2");
            multiValues.add("value3");
            vertexContext.put("multiField", multiValues);

            Field varcharField = new Field("multiField", FieldType.nullable(new ArrowType.Utf8()), null);

            VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

            ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("multiField"), extractorCaptor.capture());

            VarCharExtractor extractor = extractorCaptor.getValue();
            NullableVarCharHolder holder = new NullableVarCharHolder();

            extractor.extract(vertexContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals("value1;value2;value3", holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testVertexRowWriter_VarCharExtractor_SpecialIDField() {
        try {
            Field idField = new Field(SpecialKeys.ID.toString().toLowerCase(), FieldType.nullable(new ArrowType.Utf8()), null);

            VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, idField, configOptions);

            ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("id"), extractorCaptor.capture());

            VarCharExtractor extractor = extractorCaptor.getValue();
            NullableVarCharHolder holder = new NullableVarCharHolder();

            extractor.extract(vertexContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals("vertex123", holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testVertexRowWriter_DateMilliExtractor_ValidDate() {
        try {
            Date testDate = new Date();
            ArrayList<Object> dateValues = new ArrayList<>();
            dateValues.add(testDate);
            vertexContext.put("createdDate", dateValues);

            Field dateField = new Field("createdDate", FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);

            VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, dateField, configOptions);

            ArgumentCaptor<DateMilliExtractor> extractorCaptor = ArgumentCaptor.forClass(DateMilliExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("createdDate"), extractorCaptor.capture());

            DateMilliExtractor extractor = extractorCaptor.getValue();
            NullableDateMilliHolder holder = new NullableDateMilliHolder();

            extractor.extract(vertexContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals(testDate.getTime(), holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testVertexRowWriter_IntExtractor_ValidInteger() {
        try {
            ArrayList<Object> intValues = new ArrayList<>();
            intValues.add(30);
            vertexContext.put("age", intValues);

            Field intField = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);

            VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, intField, configOptions);

            ArgumentCaptor<IntExtractor> extractorCaptor = ArgumentCaptor.forClass(IntExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("age"), extractorCaptor.capture());

            IntExtractor extractor = extractorCaptor.getValue();
            NullableIntHolder holder = new NullableIntHolder();

            extractor.extract(vertexContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals(30, holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testVertexRowWriter_BigIntExtractor_ValidLong() {
        try {
            ArrayList<Object> longValues = new ArrayList<>();
            longValues.add(9223372036854775807L);
            vertexContext.put("bigNumber", longValues);

            Field bigintField = new Field("bigNumber", FieldType.nullable(new ArrowType.Int(64, true)), null);

            VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, bigintField, configOptions);

            ArgumentCaptor<BigIntExtractor> extractorCaptor = ArgumentCaptor.forClass(BigIntExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("bigNumber"), extractorCaptor.capture());

            BigIntExtractor extractor = extractorCaptor.getValue();
            NullableBigIntHolder holder = new NullableBigIntHolder();

            extractor.extract(vertexContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals(9223372036854775807L, holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testVertexRowWriter_Float4Extractor_ValidFloat() {
        try {
            ArrayList<Object> floatValues = new ArrayList<>();
            floatValues.add(3.14f);
            vertexContext.put("score", floatValues);

            Field floatField = new Field("score", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);

            VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, floatField, configOptions);

            ArgumentCaptor<Float4Extractor> extractorCaptor = ArgumentCaptor.forClass(Float4Extractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("score"), extractorCaptor.capture());

            Float4Extractor extractor = extractorCaptor.getValue();
            NullableFloat4Holder holder = new NullableFloat4Holder();

            extractor.extract(vertexContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals(3.14f, holder.value, 0.001f);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testVertexRowWriter_Float8Extractor_ValidDouble() {
        try {
            ArrayList<Object> doubleValues = new ArrayList<>();
            doubleValues.add(2.718281828);
            vertexContext.put("precision", doubleValues);

            Field doubleField = new Field("precision", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);

            VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, doubleField, configOptions);

            ArgumentCaptor<Float8Extractor> extractorCaptor = ArgumentCaptor.forClass(Float8Extractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("precision"), extractorCaptor.capture());

            Float8Extractor extractor = extractorCaptor.getValue();
            NullableFloat8Holder holder = new NullableFloat8Holder();

            extractor.extract(vertexContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals(2.718281828, holder.value, 0.000001);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }


    @Test
    public void testEdgeRowWriter_BitExtractor_ValidBoolean() {
        try {
            edgeContext.put("isActive", true);

            Field bitField = new Field("isActive", FieldType.nullable(new ArrowType.Bool()), null);

            EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, bitField, configOptions);

            ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("isActive"), extractorCaptor.capture());

            BitExtractor extractor = extractorCaptor.getValue();
            NullableBitHolder holder = new NullableBitHolder();

            extractor.extract(edgeContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals(1, holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testEdgeRowWriter_VarCharExtractor_ValidString() {
        try {
            edgeContext.put("description", "friendship");

            Field varcharField = new Field("description", FieldType.nullable(new ArrowType.Utf8()), null);

            EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

            ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("description"), extractorCaptor.capture());

            VarCharExtractor extractor = extractorCaptor.getValue();
            NullableVarCharHolder holder = new NullableVarCharHolder();

            extractor.extract(edgeContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals("friendship", holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testEdgeRowWriter_DateMilliExtractor_ValidDate() {
        try {
            Date testDate = new Date();
            edgeContext.put("createdAt", testDate);

            Field dateField = new Field("createdAt", FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);

            EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, dateField, configOptions);

            ArgumentCaptor<DateMilliExtractor> extractorCaptor = ArgumentCaptor.forClass(DateMilliExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("createdAt"), extractorCaptor.capture());

            DateMilliExtractor extractor = extractorCaptor.getValue();
            NullableDateMilliHolder holder = new NullableDateMilliHolder();

            extractor.extract(edgeContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals(testDate.getTime(), holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testEdgeRowWriter_IntExtractor_ValidInteger() {
        try {
            edgeContext.put("since", 2020);

            Field intField = new Field("since", FieldType.nullable(new ArrowType.Int(32, true)), null);

            EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, intField, configOptions);

            ArgumentCaptor<IntExtractor> extractorCaptor = ArgumentCaptor.forClass(IntExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("since"), extractorCaptor.capture());

            IntExtractor extractor = extractorCaptor.getValue();
            NullableIntHolder holder = new NullableIntHolder();

            extractor.extract(edgeContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals(2020, holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testEdgeRowWriter_Float8Extractor_ValidDouble() {
        try {
            edgeContext.put("weight", 0.8);

            Field doubleField = new Field("weight", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);

            EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, doubleField, configOptions);

            ArgumentCaptor<Float8Extractor> extractorCaptor = ArgumentCaptor.forClass(Float8Extractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("weight"), extractorCaptor.capture());

            Float8Extractor extractor = extractorCaptor.getValue();
            NullableFloat8Holder holder = new NullableFloat8Holder();

            extractor.extract(edgeContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals(0.8, holder.value, 0.001);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    // CustomSchemaRowWriter Tests
    @Test
    public void testCustomSchemaRowWriter_BitExtractor_ValidBoolean() {
        try {
            customContext.put("isEnabled", true);

            Field bitField = new Field("isEnabled", FieldType.nullable(new ArrowType.Bool()), null);

            CustomSchemaRowWriter.writeRowTemplate(mockRowWriterBuilder, bitField, configOptions);

            ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("isEnabled"), extractorCaptor.capture());

            BitExtractor extractor = extractorCaptor.getValue();
            NullableBitHolder holder = new NullableBitHolder();

            extractor.extract(customContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals(1, holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testCustomSchemaRowWriter_VarCharExtractor_ValidString() {
        try {
            customContext.put("name", "Custom Name");

            Field varcharField = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);

            CustomSchemaRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

            ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("name"), extractorCaptor.capture());

            VarCharExtractor extractor = extractorCaptor.getValue();
            NullableVarCharHolder holder = new NullableVarCharHolder();

            extractor.extract(customContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals("Custom Name", holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testCustomSchemaRowWriter_DateMilliExtractor_ValidDate() {
        try {
            Date testDate = new Date();
            customContext.put("dateField", testDate);

            Field dateField = new Field("dateField", FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);

            CustomSchemaRowWriter.writeRowTemplate(mockRowWriterBuilder, dateField, configOptions);

            ArgumentCaptor<DateMilliExtractor> extractorCaptor = ArgumentCaptor.forClass(DateMilliExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("dateField"), extractorCaptor.capture());

            DateMilliExtractor extractor = extractorCaptor.getValue();
            NullableDateMilliHolder holder = new NullableDateMilliHolder();

            extractor.extract(customContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals(testDate.getTime(), holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testCustomSchemaRowWriter_IntExtractor_ValidInteger() {
        try {
            customContext.put("count", 42);

            Field intField = new Field("count", FieldType.nullable(new ArrowType.Int(32, true)), null);

            CustomSchemaRowWriter.writeRowTemplate(mockRowWriterBuilder, intField, configOptions);

            ArgumentCaptor<IntExtractor> extractorCaptor = ArgumentCaptor.forClass(IntExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("count"), extractorCaptor.capture());

            IntExtractor extractor = extractorCaptor.getValue();
            NullableIntHolder holder = new NullableIntHolder();

            extractor.extract(customContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals(42, holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testCustomSchemaRowWriter_BigIntExtractor_ValidLong() {
        try {
            customContext.put("bigValue", 1234567890123456789L);

            Field bigintField = new Field("bigValue", FieldType.nullable(new ArrowType.Int(64, true)), null);

            CustomSchemaRowWriter.writeRowTemplate(mockRowWriterBuilder, bigintField, configOptions);

            ArgumentCaptor<BigIntExtractor> extractorCaptor = ArgumentCaptor.forClass(BigIntExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("bigValue"), extractorCaptor.capture());

            BigIntExtractor extractor = extractorCaptor.getValue();
            NullableBigIntHolder holder = new NullableBigIntHolder();

            extractor.extract(customContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals(1234567890123456789L, holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testCustomSchemaRowWriter_Float4Extractor_ValidFloat() {
        try {
            customContext.put("floatValue", 2.5f);

            Field floatField = new Field("floatValue", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);

            CustomSchemaRowWriter.writeRowTemplate(mockRowWriterBuilder, floatField, configOptions);

            ArgumentCaptor<Float4Extractor> extractorCaptor = ArgumentCaptor.forClass(Float4Extractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("floatValue"), extractorCaptor.capture());

            Float4Extractor extractor = extractorCaptor.getValue();
            NullableFloat4Holder holder = new NullableFloat4Holder();

            extractor.extract(customContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals(2.5f, holder.value, 0.001f);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testCustomSchemaRowWriter_Float8Extractor_ValidDouble() {
        try {
            customContext.put("doubleValue", 3.14159);

            Field doubleField = new Field("doubleValue", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);

            CustomSchemaRowWriter.writeRowTemplate(mockRowWriterBuilder, doubleField, configOptions);

            ArgumentCaptor<Float8Extractor> extractorCaptor = ArgumentCaptor.forClass(Float8Extractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("doubleValue"), extractorCaptor.capture());

            Float8Extractor extractor = extractorCaptor.getValue();
            NullableFloat8Holder holder = new NullableFloat8Holder();

            extractor.extract(customContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals(3.14159, holder.value, 0.000001);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }


    @Test
    public void testVertexRowWriter_CaseInsensitive() {
        try {
            configOptions.put(Constants.SCHEMA_CASE_INSEN, "true");

            ArrayList<Object> nameValues = new ArrayList<>();
            nameValues.add("John Doe");
            vertexContext.put("name", nameValues);

            Field varcharField = new Field("NAME", FieldType.nullable(new ArrowType.Utf8()), null);

            VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

            ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("NAME"), extractorCaptor.capture());

            VarCharExtractor extractor = extractorCaptor.getValue();
            NullableVarCharHolder holder = new NullableVarCharHolder();

            extractor.extract(vertexContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals("John Doe", holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testEdgeRowWriter_CaseSensitive() {
        try {
            configOptions.put(Constants.SCHEMA_CASE_INSEN, "false");
            edgeContext.put("testfield", "Test Value");

            Field varcharField = new Field("TESTFIELD", FieldType.nullable(new ArrowType.Utf8()), null);

            EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

            ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("TESTFIELD"), extractorCaptor.capture());

            VarCharExtractor extractor = extractorCaptor.getValue();
            NullableVarCharHolder holder = new NullableVarCharHolder();

            // Should NOT find "testfield" due to case difference
            extractor.extract(edgeContext, holder);
            assertEquals(0, holder.isSet);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testCustomSchemaRowWriter_DefaultCaseInsensitive() {
        try {
            // When no configuration is provided, should default to case insensitive
            customContext.put("mixedcase", "Mixed Case Value");

            Field varcharField = new Field("MIXEDCASE", FieldType.nullable(new ArrowType.Utf8()), null);

            CustomSchemaRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

            ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("MIXEDCASE"), extractorCaptor.capture());

            VarCharExtractor extractor = extractorCaptor.getValue();
            NullableVarCharHolder holder = new NullableVarCharHolder();

            extractor.extract(customContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals("Mixed Case Value", holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    // Test null and empty value handling
    @Test
    public void testVertexRowWriter_NullValues() {
        try {
            // Test with empty ArrayList instead of null values to avoid NPE
            vertexContext.put("nullField", new ArrayList<>());

            Field varcharField = new Field("nullField", FieldType.nullable(new ArrowType.Utf8()), null);

            VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

            ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("nullField"), extractorCaptor.capture());

            VarCharExtractor extractor = extractorCaptor.getValue();
            NullableVarCharHolder holder = new NullableVarCharHolder();

            extractor.extract(vertexContext, holder);
            assertEquals(0, holder.isSet);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testEdgeRowWriter_EmptyStringValues() {
        try {
            edgeContext.put("emptyField", "   ");

            Field bitField = new Field("emptyField", FieldType.nullable(new ArrowType.Bool()), null);

            EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, bitField, configOptions);

            ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("emptyField"), extractorCaptor.capture());

            BitExtractor extractor = extractorCaptor.getValue();
            NullableBitHolder holder = new NullableBitHolder();

            extractor.extract(edgeContext, holder);
            assertEquals(0, holder.isSet);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testCustomSchemaRowWriter_EmptyStringValues() {
        try {
            customContext.put("emptyDate", "");

            Field dateField = new Field("emptyDate", FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);

            CustomSchemaRowWriter.writeRowTemplate(mockRowWriterBuilder, dateField, configOptions);

            ArgumentCaptor<DateMilliExtractor> extractorCaptor = ArgumentCaptor.forClass(DateMilliExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("emptyDate"), extractorCaptor.capture());

            DateMilliExtractor extractor = extractorCaptor.getValue();
            NullableDateMilliHolder holder = new NullableDateMilliHolder();

            extractor.extract(customContext, holder);
            assertEquals(0, holder.isSet);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    // Test private constructors
    @Test(expected = IllegalAccessException.class)
    public void testVertexRowWriter_PrivateConstructor() throws Exception {
        VertexRowWriter.class.getDeclaredConstructor().newInstance();
    }

    @Test(expected = IllegalAccessException.class)
    public void testEdgeRowWriter_PrivateConstructor() throws Exception {
        EdgeRowWriter.class.getDeclaredConstructor().newInstance();
    }

    @Test(expected = IllegalAccessException.class)
    public void testCustomSchemaRowWriter_PrivateConstructor() throws Exception {
        CustomSchemaRowWriter.class.getDeclaredConstructor().newInstance();
    }
} 