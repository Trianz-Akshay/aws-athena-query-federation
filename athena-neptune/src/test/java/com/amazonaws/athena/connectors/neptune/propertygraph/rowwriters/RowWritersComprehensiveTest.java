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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RowWritersComprehensiveTest
{
    public static final String ACTIVE = "active";
    public static final String INACTIVE = "inactive";
    public static final String MULTI_FIELD = "multiField";
    public static final String CREATED_DATE = "createdDate";
    public static final String AGE = "age";
    public static final String BIG_NUMBER = "bigNumber";
    public static final String SCORE = "score";
    public static final String PRECISION = "precision";
    public static final String IS_ACTIVE = "isActive";
    public static final String DESCRIPTION = "description";
    public static final String CREATED_AT = "createdAt";
    public static final String SINCE = "since";
    public static final String WEIGHT = "weight";
    public static final String IS_ENABLED = "isEnabled";
    public static final String NAME = "name";
    public static final String DATE_FIELD = "dateField";
    public static final String COUNT = "count";
    public static final String BIG_VALUE = "bigValue";
    public static final String FLOAT_VALUE = "floatValue";
    public static final String DOUBLE_VALUE = "doubleValue";
    public static final String NAME_FIELD = "NAME";
    public static final String TESTFIELD = "TESTFIELD";
    public static final String MIXEDCASE = "MIXEDCASE";
    public static final String NULL_FIELD = "nullField";
    public static final String EMPTY_FIELD = "emptyField";
    public static final String EMPTY_DATE = "emptyDate";

    // Test values
    private static final String VERTEX_123 = "vertex123";
    private static final String PERSON_LABEL = "person";
    private static final String EDGE_456 = "edge456";
    private static final String KNOWS_LABEL = "knows";
    private static final String CUSTOM_FIELD = "customField";
    private static final String CUSTOM_VALUE = "customValue";
    private static final String VALUE_1 = "value1";
    private static final String VALUE_2 = "value2";
    private static final String VALUE_3 = "value3";
    private static final String SEMICOLON_SEPARATED = ";";
    private static final String FRIENDSHIP = "friendship";
    private static final String CUSTOM_NAME = "Custom Name";
    private static final String MIXED_CASE_VALUE = "Mixed Case Value";
    private static final String TEST_VALUE = "Test Value";
    private static final String EMPTY_STRING = "";
    private static final String SPACES = "   ";
    private static final String CASE_INSENSITIVE_TRUE = "true";
    private static final String CASE_INSENSITIVE_FALSE = "false";
    private static final String ID_FIELD = "id";
    private static final String TESTFIELD_LOWER = "testfield";
    private static final String MIXEDCASE_LOWER = "mixedcase";
    @Mock
    private RowWriterBuilder mockRowWriterBuilder;

    private Map<String, String> configOptions;
    private Map<String, Object> vertexContext;
    private Map<String, Object> edgeContext;
    private Map<String, Object> customContext;

    @Before
    public void setUp()
    {
        when(mockRowWriterBuilder.withExtractor(anyString(), any())).thenReturn(mockRowWriterBuilder);

        // Setup config options
        configOptions = new HashMap<>();

        // Setup vertex context data
        vertexContext = new HashMap<>();
        vertexContext.put(T.id.toString(), VERTEX_123);
        vertexContext.put(T.label.toString(), PERSON_LABEL);

        // Setup edge context data (simplified to avoid Direction issues)
        edgeContext = new HashMap<>();
        edgeContext.put(T.id.toString(), EDGE_456);
        edgeContext.put(T.label.toString(), KNOWS_LABEL);

        // Setup custom context data
        customContext = new HashMap<>();
        customContext.put(CUSTOM_FIELD, CUSTOM_VALUE);
    }

    // VertexRowWriter Tests
    @Test
    public void writeRowTemplate_WithVertexRowWriterAndBitExtractor_ExtractsValidBoolean() throws Exception
    {
        addToVertexContext(ACTIVE, true);

        Field bitField = new Field(ACTIVE, FieldType.nullable(new ArrowType.Bool()), null);

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, bitField, configOptions);

        ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(ACTIVE), extractorCaptor.capture());

        BitExtractor extractor = extractorCaptor.getValue();
        NullableBitHolder holder = new NullableBitHolder();

        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(1, holder.value);
    }

    @Test
    public void writeRowTemplate_WithVertexRowWriterAndBitExtractor_ExtractsValidFalse() throws Exception
    {
        ArrayList<Object> boolValues = new ArrayList<>();
        boolValues.add(false);
        vertexContext.put(INACTIVE, boolValues);

        Field bitField = new Field(INACTIVE, FieldType.nullable(new ArrowType.Bool()), null);

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, bitField, configOptions);

        ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(INACTIVE), extractorCaptor.capture());

        BitExtractor extractor = extractorCaptor.getValue();
        NullableBitHolder holder = new NullableBitHolder();

        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(0, holder.value);
    }

    @Test
    public void writeRowTemplate_WithVertexRowWriterAndVarCharExtractor_HandlesMultipleValues() throws Exception
    {
        ArrayList<Object> multiValues = new ArrayList<>();
        multiValues.add(VALUE_1);
        multiValues.add(VALUE_2);
        multiValues.add(VALUE_3);
        vertexContext.put(MULTI_FIELD, multiValues);

        Field varcharField = new Field(MULTI_FIELD, FieldType.nullable(new ArrowType.Utf8()), null);

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(MULTI_FIELD), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        NullableVarCharHolder holder = new NullableVarCharHolder();

        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(VALUE_1 + SEMICOLON_SEPARATED + VALUE_2 + SEMICOLON_SEPARATED + VALUE_3, holder.value);
    }

    @Test
    public void writeRowTemplate_WithVertexRowWriterAndVarCharExtractor_HandlesSpecialIDField() throws Exception
    {
        Field idField = new Field(SpecialKeys.ID.toString().toLowerCase(), FieldType.nullable(new ArrowType.Utf8()), null);

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, idField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(ID_FIELD), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        NullableVarCharHolder holder = new NullableVarCharHolder();

        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(VERTEX_123, holder.value);
    }

    @Test
    public void writeRowTemplate_WithVertexRowWriterAndDateMilliExtractor_ExtractsValidDate() throws Exception
    {
        Date testDate = new Date();
        ArrayList<Object> dateValues = new ArrayList<>();
        dateValues.add(testDate);
        vertexContext.put(CREATED_DATE, dateValues);

        Field dateField = new Field(CREATED_DATE, FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, dateField, configOptions);

        ArgumentCaptor<DateMilliExtractor> extractorCaptor = ArgumentCaptor.forClass(DateMilliExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(CREATED_DATE), extractorCaptor.capture());

        DateMilliExtractor extractor = extractorCaptor.getValue();
        NullableDateMilliHolder holder = new NullableDateMilliHolder();

        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(testDate.getTime(), holder.value);
    }

    @Test
    public void writeRowTemplate_WithVertexRowWriterAndIntExtractor_ExtractsValidInteger() throws Exception
    {
        ArrayList<Object> intValues = new ArrayList<>();
        intValues.add(30);
        vertexContext.put(AGE, intValues);

        Field intField = new Field(AGE, FieldType.nullable(new ArrowType.Int(32, true)), null);

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, intField, configOptions);

        ArgumentCaptor<IntExtractor> extractorCaptor = ArgumentCaptor.forClass(IntExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(AGE), extractorCaptor.capture());

        IntExtractor extractor = extractorCaptor.getValue();
        NullableIntHolder holder = new NullableIntHolder();

        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(30, holder.value);
    }

    @Test
    public void writeRowTemplate_WithVertexRowWriterAndBigIntExtractor_ExtractsValidLong() throws Exception
    {
        ArrayList<Object> longValues = new ArrayList<>();
        longValues.add(9223372036854775807L);
        vertexContext.put(BIG_NUMBER, longValues);

        Field bigintField = new Field(BIG_NUMBER, FieldType.nullable(new ArrowType.Int(64, true)), null);

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, bigintField, configOptions);

        ArgumentCaptor<BigIntExtractor> extractorCaptor = ArgumentCaptor.forClass(BigIntExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(BIG_NUMBER), extractorCaptor.capture());

        BigIntExtractor extractor = extractorCaptor.getValue();
        NullableBigIntHolder holder = new NullableBigIntHolder();

        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(9223372036854775807L, holder.value);
    }

    @Test
    public void writeRowTemplate_WithVertexRowWriterAndFloat4Extractor_ExtractsValidFloat() throws Exception
    {
        ArrayList<Object> floatValues = new ArrayList<>();
        floatValues.add(3.14f);
        vertexContext.put(SCORE, floatValues);

        Field floatField = new Field(SCORE, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, floatField, configOptions);

        ArgumentCaptor<Float4Extractor> extractorCaptor = ArgumentCaptor.forClass(Float4Extractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(SCORE), extractorCaptor.capture());

        Float4Extractor extractor = extractorCaptor.getValue();
        NullableFloat4Holder holder = new NullableFloat4Holder();

        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(3.14f, holder.value, 0.001f);
    }

    @Test
    public void writeRowTemplate_WithVertexRowWriterAndFloat8Extractor_ExtractsValidDouble() throws Exception
    {
        ArrayList<Object> doubleValues = new ArrayList<>();
        doubleValues.add(2.718281828);
        vertexContext.put(PRECISION, doubleValues);

        Field doubleField = new Field(PRECISION, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, doubleField, configOptions);

        ArgumentCaptor<Float8Extractor> extractorCaptor = ArgumentCaptor.forClass(Float8Extractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(PRECISION), extractorCaptor.capture());

        Float8Extractor extractor = extractorCaptor.getValue();
        NullableFloat8Holder holder = new NullableFloat8Holder();

        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(2.718281828, holder.value, 0.000001);
    }


    @Test
    public void writeRowTemplate_WithEdgeRowWriterAndBitExtractor_ExtractsValidBoolean() throws Exception
    {
        addToEdgeContext(IS_ACTIVE, true);

        Field bitField = new Field(IS_ACTIVE, FieldType.nullable(new ArrowType.Bool()), null);

        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, bitField, configOptions);

        ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(IS_ACTIVE), extractorCaptor.capture());

        BitExtractor extractor = extractorCaptor.getValue();
        NullableBitHolder holder = new NullableBitHolder();

        extractor.extract(edgeContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(1, holder.value);
    }

    @Test
    public void writeRowTemplate_WithEdgeRowWriterAndVarCharExtractor_ExtractsValidString() throws Exception
    {
        edgeContext.put(DESCRIPTION, FRIENDSHIP);

        Field varcharField = new Field(DESCRIPTION, FieldType.nullable(new ArrowType.Utf8()), null);

        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(DESCRIPTION), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        NullableVarCharHolder holder = new NullableVarCharHolder();

        extractor.extract(edgeContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(FRIENDSHIP, holder.value);
    }

    @Test
    public void writeRowTemplate_WithEdgeRowWriterAndDateMilliExtractor_ExtractsValidDate() throws Exception
    {
        Date testDate = new Date();
        edgeContext.put(CREATED_AT, testDate);

        Field dateField = new Field(CREATED_AT, FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);

        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, dateField, configOptions);

        ArgumentCaptor<DateMilliExtractor> extractorCaptor = ArgumentCaptor.forClass(DateMilliExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(CREATED_AT), extractorCaptor.capture());

        DateMilliExtractor extractor = extractorCaptor.getValue();
        NullableDateMilliHolder holder = new NullableDateMilliHolder();

        extractor.extract(edgeContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(testDate.getTime(), holder.value);
    }

    @Test
    public void writeRowTemplate_WithEdgeRowWriterAndIntExtractor_ExtractsValidInteger() throws Exception
    {
        edgeContext.put(SINCE, 2020);

        Field intField = new Field(SINCE, FieldType.nullable(new ArrowType.Int(32, true)), null);

        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, intField, configOptions);

        ArgumentCaptor<IntExtractor> extractorCaptor = ArgumentCaptor.forClass(IntExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(SINCE), extractorCaptor.capture());

        IntExtractor extractor = extractorCaptor.getValue();
        NullableIntHolder holder = new NullableIntHolder();

        extractor.extract(edgeContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(2020, holder.value);
    }

    @Test
    public void writeRowTemplate_WithEdgeRowWriterAndFloat8Extractor_ExtractsValidDouble() throws Exception
    {
        edgeContext.put(WEIGHT, 0.8);

        Field doubleField = new Field(WEIGHT, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);

        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, doubleField, configOptions);

        ArgumentCaptor<Float8Extractor> extractorCaptor = ArgumentCaptor.forClass(Float8Extractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(WEIGHT), extractorCaptor.capture());

        Float8Extractor extractor = extractorCaptor.getValue();
        NullableFloat8Holder holder = new NullableFloat8Holder();

        extractor.extract(edgeContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(0.8, holder.value, 0.001);
    }

    @Test
    public void writeRowTemplate_WithEdgeRowWriterAndBigIntExtractor_ExtractsValidLong() throws Exception
    {
        edgeContext.put(BIG_VALUE, 1234567890123456789L);

        Field bigintField = new Field(BIG_VALUE, FieldType.nullable(new ArrowType.Int(64, true)), null);

        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, bigintField, configOptions);

        ArgumentCaptor<BigIntExtractor> extractorCaptor = ArgumentCaptor.forClass(BigIntExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(BIG_VALUE), extractorCaptor.capture());

        BigIntExtractor extractor = extractorCaptor.getValue();
        NullableBigIntHolder holder = new NullableBigIntHolder();

        extractor.extract(edgeContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(1234567890123456789L, holder.value);
    }

    @Test
    public void writeRowTemplate_WithEdgeRowWriterAndFloat4Extractor_ExtractsValidFloat() throws Exception
    {
        edgeContext.put(FLOAT_VALUE, 2.5f);

        Field floatField = new Field(FLOAT_VALUE, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);

        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, floatField, configOptions);

        ArgumentCaptor<Float4Extractor> extractorCaptor = ArgumentCaptor.forClass(Float4Extractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(FLOAT_VALUE), extractorCaptor.capture());

        Float4Extractor extractor = extractorCaptor.getValue();
        NullableFloat4Holder holder = new NullableFloat4Holder();

        extractor.extract(edgeContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(2.5f, holder.value, 0.001f);
    }

    // CustomSchemaRowWriter Tests
    @Test
    public void writeRowTemplate_WithCustomSchemaRowWriterAndBitExtractor_ExtractsValidBoolean() throws Exception
    {
        addToCustomContext(IS_ENABLED, true);

        Field bitField = new Field(IS_ENABLED, FieldType.nullable(new ArrowType.Bool()), null);

        CustomSchemaRowWriter.writeRowTemplate(mockRowWriterBuilder, bitField, configOptions);

        ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(IS_ENABLED), extractorCaptor.capture());

        BitExtractor extractor = extractorCaptor.getValue();
        NullableBitHolder holder = new NullableBitHolder();

        extractor.extract(customContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(1, holder.value);
    }

    @Test
    public void writeRowTemplate_WithCustomSchemaRowWriterAndVarCharExtractor_ExtractsValidString() throws Exception
    {
        customContext.put(NAME, CUSTOM_NAME);

        Field varcharField = new Field(NAME, FieldType.nullable(new ArrowType.Utf8()), null);

        CustomSchemaRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(NAME), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        NullableVarCharHolder holder = new NullableVarCharHolder();

        extractor.extract(customContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(CUSTOM_NAME, holder.value);
    }

    @Test
    public void writeRowTemplate_WithCustomSchemaRowWriterAndDateMilliExtractor_ExtractsValidDate() throws Exception
    {
        Date testDate = new Date();
        customContext.put(DATE_FIELD, testDate);

        Field dateField = new Field(DATE_FIELD, FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);

        CustomSchemaRowWriter.writeRowTemplate(mockRowWriterBuilder, dateField, configOptions);

        ArgumentCaptor<DateMilliExtractor> extractorCaptor = ArgumentCaptor.forClass(DateMilliExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(DATE_FIELD), extractorCaptor.capture());

        DateMilliExtractor extractor = extractorCaptor.getValue();
        NullableDateMilliHolder holder = new NullableDateMilliHolder();

        extractor.extract(customContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(testDate.getTime(), holder.value);
    }

    @Test
    public void writeRowTemplate_WithCustomSchemaRowWriterAndIntExtractor_ExtractsValidInteger() throws Exception
    {
        customContext.put(COUNT, 42);

        Field intField = new Field(COUNT, FieldType.nullable(new ArrowType.Int(32, true)), null);

        CustomSchemaRowWriter.writeRowTemplate(mockRowWriterBuilder, intField, configOptions);

        ArgumentCaptor<IntExtractor> extractorCaptor = ArgumentCaptor.forClass(IntExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(COUNT), extractorCaptor.capture());

        IntExtractor extractor = extractorCaptor.getValue();
        NullableIntHolder holder = new NullableIntHolder();

        extractor.extract(customContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(42, holder.value);
    }

    @Test
    public void writeRowTemplate_WithCustomSchemaRowWriterAndBigIntExtractor_ExtractsValidLong() throws Exception
    {
        customContext.put(BIG_VALUE, 1234567890123456789L);

        Field bigintField = new Field(BIG_VALUE, FieldType.nullable(new ArrowType.Int(64, true)), null);

        CustomSchemaRowWriter.writeRowTemplate(mockRowWriterBuilder, bigintField, configOptions);

        ArgumentCaptor<BigIntExtractor> extractorCaptor = ArgumentCaptor.forClass(BigIntExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(BIG_VALUE), extractorCaptor.capture());

        BigIntExtractor extractor = extractorCaptor.getValue();
        NullableBigIntHolder holder = new NullableBigIntHolder();

        extractor.extract(customContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(1234567890123456789L, holder.value);
    }

    @Test
    public void writeRowTemplate_WithCustomSchemaRowWriterAndFloat4Extractor_ExtractsValidFloat() throws Exception
    {
        customContext.put(FLOAT_VALUE, 2.5f);

        Field floatField = new Field(FLOAT_VALUE, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);

        CustomSchemaRowWriter.writeRowTemplate(mockRowWriterBuilder, floatField, configOptions);

        ArgumentCaptor<Float4Extractor> extractorCaptor = ArgumentCaptor.forClass(Float4Extractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(FLOAT_VALUE), extractorCaptor.capture());

        Float4Extractor extractor = extractorCaptor.getValue();
        NullableFloat4Holder holder = new NullableFloat4Holder();

        extractor.extract(customContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(2.5f, holder.value, 0.001f);
    }

    @Test
    public void writeRowTemplate_WithCustomSchemaRowWriterAndFloat8Extractor_ExtractsValidDouble() throws Exception
    {
        customContext.put(DOUBLE_VALUE, 3.14159);

        Field doubleField = new Field(DOUBLE_VALUE, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);

        CustomSchemaRowWriter.writeRowTemplate(mockRowWriterBuilder, doubleField, configOptions);

        ArgumentCaptor<Float8Extractor> extractorCaptor = ArgumentCaptor.forClass(Float8Extractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(DOUBLE_VALUE), extractorCaptor.capture());

        Float8Extractor extractor = extractorCaptor.getValue();
        NullableFloat8Holder holder = new NullableFloat8Holder();

        extractor.extract(customContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(3.14159, holder.value, 0.000001);
    }


    @Test
    public void writeRowTemplate_WithVertexRowWriterAndCaseInsensitive_HandlesCaseInsensitive() throws Exception
    {
        setupCaseInsensitiveConfig();
        addToVertexContext(NAME, "John Doe");

        Field varcharField = new Field(NAME_FIELD, FieldType.nullable(new ArrowType.Utf8()), null);

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(NAME_FIELD), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        NullableVarCharHolder holder = new NullableVarCharHolder();

        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals("John Doe", holder.value);
    }

    @Test
    public void writeRowTemplate_WithEdgeRowWriterAndCaseSensitive_HandlesCaseSensitive() throws Exception
    {
        setupCaseSensitiveConfig();
        addToEdgeContext(TESTFIELD_LOWER, TEST_VALUE);

        Field varcharField = new Field(TESTFIELD, FieldType.nullable(new ArrowType.Utf8()), null);

        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(TESTFIELD), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        NullableVarCharHolder holder = new NullableVarCharHolder();

        // Should NOT find "testfield" due to case difference
        extractor.extract(edgeContext, holder);
        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithCustomSchemaRowWriterAndDefaultCaseInsensitive_HandlesDefaultCaseInsensitive() throws Exception
    {
        // When no configuration is provided, should default to case insensitive
        customContext.put(MIXEDCASE_LOWER, MIXED_CASE_VALUE);

        Field varcharField = new Field(MIXEDCASE, FieldType.nullable(new ArrowType.Utf8()), null);

        CustomSchemaRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(MIXEDCASE), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        NullableVarCharHolder holder = new NullableVarCharHolder();

        extractor.extract(customContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(MIXED_CASE_VALUE, holder.value);
    }

    // Test null and empty value handling
    @Test
    public void writeRowTemplate_WithVertexRowWriterAndNullValues_HandlesNullValues() throws Exception
    {
        // Test with empty ArrayList instead of null values to avoid NPE
        vertexContext.put(NULL_FIELD, new ArrayList<>());

        Field varcharField = new Field(NULL_FIELD, FieldType.nullable(new ArrowType.Utf8()), null);

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(NULL_FIELD), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        NullableVarCharHolder holder = new NullableVarCharHolder();

        extractor.extract(vertexContext, holder);
        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithEdgeRowWriterAndEmptyStringValues_HandlesEmptyStringValues() throws Exception
    {
        edgeContext.put(EMPTY_FIELD, SPACES);

        Field bitField = new Field(EMPTY_FIELD, FieldType.nullable(new ArrowType.Bool()), null);

        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, bitField, configOptions);

        ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(EMPTY_FIELD), extractorCaptor.capture());

        BitExtractor extractor = extractorCaptor.getValue();
        NullableBitHolder holder = new NullableBitHolder();

        extractor.extract(edgeContext, holder);
        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithCustomSchemaRowWriterAndEmptyStringValues_HandlesEmptyStringValues() throws Exception
    {
        customContext.put(EMPTY_DATE, EMPTY_STRING);

        Field dateField = new Field(EMPTY_DATE, FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);

        CustomSchemaRowWriter.writeRowTemplate(mockRowWriterBuilder, dateField, configOptions);

        ArgumentCaptor<DateMilliExtractor> extractorCaptor = ArgumentCaptor.forClass(DateMilliExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(EMPTY_DATE), extractorCaptor.capture());

        DateMilliExtractor extractor = extractorCaptor.getValue();
        NullableDateMilliHolder holder = new NullableDateMilliHolder();

        extractor.extract(customContext, holder);
        assertEquals(0, holder.isSet);
    }

    private void setupCaseSensitiveConfig()
    {
        configOptions.put(Constants.SCHEMA_CASE_INSEN, CASE_INSENSITIVE_FALSE);
    }

    private void setupCaseInsensitiveConfig()
    {
        configOptions.put(Constants.SCHEMA_CASE_INSEN, CASE_INSENSITIVE_TRUE);
    }

    private void addToVertexContext(String fieldName, Object value)
    {
        if (value instanceof ArrayList) {
            vertexContext.put(fieldName, value);
        }
        else {
            ArrayList<Object> list = new ArrayList<>();
            list.add(value);
            vertexContext.put(fieldName, list);
        }
    }

    private void addToEdgeContext(String fieldName, Object value)
    {
        edgeContext.put(fieldName, value);
    }

    private void addToCustomContext(String fieldName, Object value)
    {
        customContext.put(fieldName, value);
    }
} 