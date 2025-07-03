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
public class VertexRowWriterTest {

    @Mock
    private RowWriterBuilder mockRowWriterBuilder;

    private Map<String, String> configOptions;
    private Map<String, Object> vertexContext;

    @Before
    public void setUp() {
        when(mockRowWriterBuilder.withExtractor(anyString(), any())).thenReturn(mockRowWriterBuilder);

        // Setup config options
        configOptions = new HashMap<>();

        // Setup vertex context data
        vertexContext = new HashMap<>();
        vertexContext.put(T.id.toString(), "vertex123");
        vertexContext.put(T.label.toString(), "person");
    }

    @Test
    public void testBitExtractor_ValidBoolean() {
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
    public void testBitExtractor_ValidFalse() {
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
    public void testBitExtractor_NullValue() {
        try {
            vertexContext.put("nullField", null);

            Field bitField = new Field("nullField", FieldType.nullable(new ArrowType.Bool()), null);

            VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, bitField, configOptions);

            ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("nullField"), extractorCaptor.capture());

            BitExtractor extractor = extractorCaptor.getValue();
            NullableBitHolder holder = new NullableBitHolder();

            extractor.extract(vertexContext, holder);
            assertEquals(0, holder.isSet);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testBitExtractor_EmptyString() {
        try {
            ArrayList<Object> emptyValues = new ArrayList<>();
            emptyValues.add("   ");
            vertexContext.put("emptyField", emptyValues);

            Field bitField = new Field("emptyField", FieldType.nullable(new ArrowType.Bool()), null);

            VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, bitField, configOptions);

            ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("emptyField"), extractorCaptor.capture());

            BitExtractor extractor = extractorCaptor.getValue();
            NullableBitHolder holder = new NullableBitHolder();

            extractor.extract(vertexContext, holder);
            assertEquals(0, holder.isSet);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testVarCharExtractor_RegularField() {
        try {
            ArrayList<Object> nameValues = new ArrayList<>();
            nameValues.add("John Doe");
            vertexContext.put("name", nameValues);

            Field varcharField = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);

            VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

            ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("name"), extractorCaptor.capture());

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
    public void testVarCharExtractor_SpecialIDField() {
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
    public void testVarCharExtractor_MultipleValues() {
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
    public void testVarCharExtractor_EmptyArrayList() {
        try {
            vertexContext.put("emptyList", new ArrayList<>());

            Field varcharField = new Field("emptyList", FieldType.nullable(new ArrowType.Utf8()), null);

            VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

            ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("emptyList"), extractorCaptor.capture());

            VarCharExtractor extractor = extractorCaptor.getValue();
            NullableVarCharHolder holder = new NullableVarCharHolder();

            extractor.extract(vertexContext, holder);
            assertEquals(0, holder.isSet);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testDateMilliExtractor_ValidDate() {
        try {
            Date testDate = new Date();
            ArrayList<Object> dateValues = new ArrayList<>();
            dateValues.add(testDate);
            vertexContext.put("createdDate", dateValues);

            Field dateField = new Field("createdDate", FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()), null);

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
    public void testIntExtractor_ValidInteger() {
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
    public void testBigIntExtractor_ValidLong() {
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
    public void testFloat4Extractor_ValidFloat() {
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
    public void testFloat8Extractor_ValidDouble() {
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
    public void testContextAsMap_CaseInsensitive() {
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

            // Should find "name" field despite case difference
            extractor.extract(vertexContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals("John Doe", holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testContextAsMap_CaseSensitive() {
        try {
            configOptions.put(Constants.SCHEMA_CASE_INSEN, "false");

            ArrayList<Object> nameValues = new ArrayList<>();
            nameValues.add("John Doe");
            vertexContext.put("name", nameValues);

            Field varcharField = new Field("NAME", FieldType.nullable(new ArrowType.Utf8()), null);

            VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

            ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("NAME"), extractorCaptor.capture());

            VarCharExtractor extractor = extractorCaptor.getValue();
            NullableVarCharHolder holder = new NullableVarCharHolder();

            // Should NOT find "name" field due to case difference
            extractor.extract(vertexContext, holder);
            assertEquals(0, holder.isSet);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testContextAsMap_DefaultCaseInsensitive() {
        try {
            // When shutil.no configuration is provided, should default to case insensitive
            ArrayList<Object> nameValues = new ArrayList<>();
            nameValues.add("Test Value");
            vertexContext.put("testfield", nameValues);

            Field varcharField = new Field("TESTFIELD", FieldType.nullable(new ArrowType.Utf8()), null);

            VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

            ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("TESTFIELD"), extractorCaptor.capture());

            VarCharExtractor extractor = extractorCaptor.getValue();
            NullableVarCharHolder holder = new NullableVarCharHolder();

            extractor.extract(vertexContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals("Test Value", holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testContextAsMap_IDFieldTransformation() {
        try {
            // Test that T.id is transformed to SpecialKeys.ID
            Field idField = new Field(SpecialKeys.ID.toString().toLowerCase(), FieldType.nullable(new ArrowType.Utf8()), null);

            VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, idField, configOptions);

            ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
            verify(mockRowWriterBuilder).withExtractor(eq("id"), extractorCaptor.capture());

            VarCharExtractor extractor = extractorCaptor.getValue();
            NullableVarCharHolder holder = new NullableVarCharHolder();

            // After contextAsMap transformation, T.id should become SpecialKeys.ID
            extractor.extract(vertexContext, holder);
            assertEquals(1, holder.isSet);
            assertEquals("vertex123", holder.value);
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test(expected = IllegalAccessException.class)
    public void testPrivateConstructor() throws Exception {
        VertexRowWriter.class.getDeclaredConstructor().newInstance();
    }
} 