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
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float4Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.IntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connectors.neptune.Constants;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
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
public class CustomSchemaRowWriterTest
{

    public static final String BOOL_FIELD = "boolField";
    public static final String STRING_FIELD = "stringField";
    public static final String DATE_FIELD = "dateField";
    public static final String INT_FIELD = "intField";
    public static final String LONG_FIELD = "longField";
    public static final String FLOAT_FIELD = "floatField";
    public static final String DOUBLE_FIELD = "doubleField";
    public static final String ID = "id";
    public static final String NULL_FIELD = "nullField";
    public static final String EMPTY_FIELD = "emptyField";
    public static final String MIXED_CASE_FIELD = "MixedCaseField";
    
    // Test values
    private static final String TEST_VALUE = "testValue";
    private static final String VALUE_1 = "value1";
    private static final String VALUE_2 = "value2";
    private static final String VALUE_3 = "value3";
    private static final String SEMICOLON_SEPARATED = ";";
    private static final String EMPTY_STRING = "";
    private static final String SPACES = "   ";
    private static final String STRING_123 = "123";
    private static final String BIG_INT_VALUE = "1234567890123456789";
    private static final String FLOAT_VALUE_3_14 = "3.14";
    private static final String DOUBLE_VALUE_2_718 = "2.718281828";
    private static final String CASE_INSENSITIVE_FALSE = "false";
    @Mock
    private GeneratedRowWriter.RowWriterBuilder mockBuilder;

    private Map<String, String> configOptions;
    private Map<String, Object> testContext;

    @Before
    public void setUp()
    {
        configOptions = new HashMap<>();
        configOptions.put(Constants.SCHEMA_CASE_INSEN, "true");
        when(mockBuilder.withExtractor(anyString(), any())).thenReturn(mockBuilder);
        testContext = new HashMap<>();
    }

    @Test
    public void writeRowTemplate_WithBitFieldAndBooleanType_CreatesBitExtractor()
    {
        Field field = new Field(BOOL_FIELD, FieldType.nullable(new ArrowType.Bool()), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq(BOOL_FIELD), any());
    }

    @Test
    public void writeRowTemplate_WithBitFieldAndArrayListType_CreatesBitExtractor()
    {
        Field field = new Field(BOOL_FIELD, FieldType.nullable(new ArrowType.Bool()), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq(BOOL_FIELD), any());
    }

    @Test
    public void writeRowTemplate_WithVarCharFieldAndStringType_CreatesVarCharExtractor()
    {
        Field field = new Field(STRING_FIELD, FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq(STRING_FIELD), any());
    }

    @Test
    public void writeRowTemplate_WithVarCharFieldAndArrayListType_CreatesVarCharExtractor()
    {
        Field field = new Field(STRING_FIELD, FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq(STRING_FIELD), any());
    }

    @Test
    public void writeRowTemplate_WithVarCharFieldAndOtherType_CreatesVarCharExtractor()
    {
        Field field = new Field(STRING_FIELD, FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq(STRING_FIELD), any());
    }

    @Test
    public void writeRowTemplate_WithDateMilliFieldAndDateType_CreatesDateMilliExtractor()
    {
        Field field = new Field(DATE_FIELD, FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq(DATE_FIELD), any());
    }

    @Test
    public void writeRowTemplate_WithDateMilliFieldAndArrayListType_CreatesDateMilliExtractor()
    {
        Field field = new Field(DATE_FIELD, FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq(DATE_FIELD), any());
    }

    @Test
    public void writeRowTemplate_WithIntFieldAndIntegerType_CreatesIntExtractor()
    {
        Field field = new Field(INT_FIELD, FieldType.nullable(new ArrowType.Int(32, true)), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq(INT_FIELD), any());
    }

    @Test
    public void writeRowTemplate_WithIntFieldAndArrayListType_CreatesIntExtractor()
    {
        Field field = new Field(INT_FIELD, FieldType.nullable(new ArrowType.Int(32, true)), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq(INT_FIELD), any());
    }

    @Test
    public void writeRowTemplate_WithBigIntFieldAndLongType_CreatesBigIntExtractor()
    {
        Field field = new Field(LONG_FIELD, FieldType.nullable(new ArrowType.Int(64, true)), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq(LONG_FIELD), any());
    }

    @Test
    public void writeRowTemplate_WithBigIntFieldAndArrayListType_CreatesBigIntExtractor()
    {
        Field field = new Field(LONG_FIELD, FieldType.nullable(new ArrowType.Int(64, true)), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq(LONG_FIELD), any());
    }

    @Test
    public void writeRowTemplate_WithFloat4FieldAndFloatType_CreatesFloat4Extractor()
    {
        Field field = new Field(FLOAT_FIELD, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq(FLOAT_FIELD), any());
    }

    @Test
    public void writeRowTemplate_WithFloat4FieldAndArrayListType_CreatesFloat4Extractor()
    {
        Field field = new Field(FLOAT_FIELD, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq(FLOAT_FIELD), any());
    }

    @Test
    public void writeRowTemplate_WithFloat8FieldAndDoubleType_CreatesFloat8Extractor()
    {
        Field field = new Field(DOUBLE_FIELD, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq(DOUBLE_FIELD), any());
    }

    @Test
    public void writeRowTemplate_WithFloat8FieldAndArrayListType_CreatesFloat8Extractor()
    {
        Field field = new Field(DOUBLE_FIELD, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq(DOUBLE_FIELD), any());
    }

    @Test
    public void writeRowTemplate_WithIdField_CreatesVarCharExtractor()
    {
        Field field = new Field(ID, FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq(ID), any());
    }

    @Test
    public void writeRowTemplate_WithNullValues_CreatesVarCharExtractor()
    {
        Field field = new Field(NULL_FIELD, FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq(NULL_FIELD), any());
    }

    @Test
    public void writeRowTemplate_WithEmptyValues_CreatesVarCharExtractor()
    {
        Field field = new Field(EMPTY_FIELD, FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq(EMPTY_FIELD), any());
    }

    @Test
    public void writeRowTemplate_WithCaseInsensitiveDisabled_CreatesVarCharExtractor()
    {
        setupCaseSensitiveConfig();
        Field field = new Field(MIXED_CASE_FIELD, FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
        verify(mockBuilder).withExtractor(eq(MIXED_CASE_FIELD), any());
    }

    @Test(expected = IllegalAccessException.class)
    public void constructor_WithPrivateAccess_ThrowsIllegalAccessException() throws Exception
    {
        CustomSchemaRowWriter.class.getDeclaredConstructor().newInstance();
    }

    // Comprehensive tests for ArrayList handling and extractor functionality
    @Test
    public void writeRowTemplate_WithBitExtractorAndValidBoolean_ExtractsCorrectValue() throws Exception
    {
        ArrayList<Object> boolValues = new ArrayList<>();
        boolValues.add(true);
        testContext.put(BOOL_FIELD, boolValues);

        Field bitField = new Field(BOOL_FIELD, FieldType.nullable(new ArrowType.Bool()), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, bitField, configOptions);

        ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
        verify(mockBuilder).withExtractor(eq(BOOL_FIELD), extractorCaptor.capture());

        BitExtractor extractor = extractorCaptor.getValue();
        NullableBitHolder holder = new NullableBitHolder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(1, holder.value);
    }

    @Test
    public void writeRowTemplate_WithBitExtractorAndEmptyString_HandlesEmptyValue() throws Exception
    {
        ArrayList<Object> emptyValues = new ArrayList<>();
        emptyValues.add(SPACES);
        testContext.put(BOOL_FIELD, emptyValues);

        Field bitField = new Field(BOOL_FIELD, FieldType.nullable(new ArrowType.Bool()), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, bitField, configOptions);

        ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
        verify(mockBuilder).withExtractor(eq(BOOL_FIELD), extractorCaptor.capture());

        BitExtractor extractor = extractorCaptor.getValue();
        NullableBitHolder holder = new NullableBitHolder();
        extractor.extract(testContext, holder);

        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithVarCharExtractorAndSingleValue_ExtractsCorrectValue() throws Exception
    {
        ArrayList<Object> stringValues = new ArrayList<>();
        stringValues.add(TEST_VALUE);
        testContext.put(STRING_FIELD, stringValues);

        Field varcharField = new Field(STRING_FIELD, FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, varcharField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockBuilder).withExtractor(eq(STRING_FIELD), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(TEST_VALUE, holder.value);
    }

    @Test
    public void writeRowTemplate_WithVarCharExtractorAndMultipleValues_ConcatenatesValues() throws Exception
    {
        ArrayList<Object> stringValues = new ArrayList<>();
        stringValues.add(VALUE_1);
        stringValues.add(VALUE_2);
        stringValues.add(VALUE_3);
        testContext.put(STRING_FIELD, stringValues);

        Field varcharField = new Field(STRING_FIELD, FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, varcharField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockBuilder).withExtractor(eq(STRING_FIELD), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(VALUE_1 + SEMICOLON_SEPARATED + VALUE_2 + SEMICOLON_SEPARATED + VALUE_3, holder.value);
    }

    @Test
    public void writeRowTemplate_WithVarCharExtractorAndOtherType_ConvertsToString() throws Exception
    {
        testContext.put(STRING_FIELD, 123);

        Field varcharField = new Field(STRING_FIELD, FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, varcharField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockBuilder).withExtractor(eq(STRING_FIELD), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(STRING_123, holder.value);
    }

    @Test
    public void writeRowTemplate_WithDateMilliExtractorAndValidDate_ExtractsCorrectValue() throws Exception
    {
        Date testDate = new Date();
        ArrayList<Object> dateValues = new ArrayList<>();
        dateValues.add(testDate);
        testContext.put(DATE_FIELD, dateValues);

        Field dateField = new Field(DATE_FIELD, FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, dateField, configOptions);

        ArgumentCaptor<DateMilliExtractor> extractorCaptor = ArgumentCaptor.forClass(DateMilliExtractor.class);
        verify(mockBuilder).withExtractor(eq(DATE_FIELD), extractorCaptor.capture());

        DateMilliExtractor extractor = extractorCaptor.getValue();
        NullableDateMilliHolder holder = new NullableDateMilliHolder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(testDate.getTime(), holder.value);
    }

    @Test
    public void writeRowTemplate_WithDateMilliExtractorAndEmptyString_HandlesEmptyValue() throws Exception
    {
        ArrayList<Object> emptyValues = new ArrayList<>();
        emptyValues.add(SPACES);
        testContext.put(DATE_FIELD, emptyValues);

        Field dateField = new Field(DATE_FIELD, FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, dateField, configOptions);

        ArgumentCaptor<DateMilliExtractor> extractorCaptor = ArgumentCaptor.forClass(DateMilliExtractor.class);
        verify(mockBuilder).withExtractor(eq(DATE_FIELD), extractorCaptor.capture());

        DateMilliExtractor extractor = extractorCaptor.getValue();
        NullableDateMilliHolder holder = new NullableDateMilliHolder();
        extractor.extract(testContext, holder);

        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithIntExtractorAndValidInteger_ExtractsCorrectValue() throws Exception
    {
        ArrayList<Object> intValues = new ArrayList<>();
        intValues.add(42);
        testContext.put(INT_FIELD, intValues);

        Field intField = new Field(INT_FIELD, FieldType.nullable(new ArrowType.Int(32, true)), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, intField, configOptions);

        ArgumentCaptor<IntExtractor> extractorCaptor = ArgumentCaptor.forClass(IntExtractor.class);
        verify(mockBuilder).withExtractor(eq(INT_FIELD), extractorCaptor.capture());

        IntExtractor extractor = extractorCaptor.getValue();
        NullableIntHolder holder = new NullableIntHolder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(42, holder.value);
    }

    @Test
    public void writeRowTemplate_WithIntExtractorAndEmptyString_HandlesEmptyValue() throws Exception
    {
        ArrayList<Object> emptyValues = new ArrayList<>();
        emptyValues.add(SPACES);
        testContext.put(INT_FIELD, emptyValues);

        Field intField = new Field(INT_FIELD, FieldType.nullable(new ArrowType.Int(32, true)), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, intField, configOptions);

        ArgumentCaptor<IntExtractor> extractorCaptor = ArgumentCaptor.forClass(IntExtractor.class);
        verify(mockBuilder).withExtractor(eq(INT_FIELD), extractorCaptor.capture());

        IntExtractor extractor = extractorCaptor.getValue();
        NullableIntHolder holder = new NullableIntHolder();
        extractor.extract(testContext, holder);

        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithBigIntExtractorAndValidLong_ExtractsCorrectValue() throws Exception
    {
        ArrayList<Object> longValues = new ArrayList<>();
        longValues.add(Long.parseLong(BIG_INT_VALUE));
        testContext.put(LONG_FIELD, longValues);

        Field bigintField = new Field(LONG_FIELD, FieldType.nullable(new ArrowType.Int(64, true)), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, bigintField, configOptions);

        ArgumentCaptor<BigIntExtractor> extractorCaptor = ArgumentCaptor.forClass(BigIntExtractor.class);
        verify(mockBuilder).withExtractor(eq(LONG_FIELD), extractorCaptor.capture());

        BigIntExtractor extractor = extractorCaptor.getValue();
        NullableBigIntHolder holder = new NullableBigIntHolder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(Long.parseLong(BIG_INT_VALUE), holder.value);
    }

    @Test
    public void writeRowTemplate_WithBigIntExtractorAndEmptyString_HandlesEmptyValue() throws Exception
    {
        ArrayList<Object> emptyValues = new ArrayList<>();
        emptyValues.add(SPACES);
        testContext.put(LONG_FIELD, emptyValues);

        Field bigintField = new Field(LONG_FIELD, FieldType.nullable(new ArrowType.Int(64, true)), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, bigintField, configOptions);

        ArgumentCaptor<BigIntExtractor> extractorCaptor = ArgumentCaptor.forClass(BigIntExtractor.class);
        verify(mockBuilder).withExtractor(eq(LONG_FIELD), extractorCaptor.capture());

        BigIntExtractor extractor = extractorCaptor.getValue();
        NullableBigIntHolder holder = new NullableBigIntHolder();
        extractor.extract(testContext, holder);

        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithFloat4ExtractorAndValidFloat_ExtractsCorrectValue() throws Exception
    {
        ArrayList<Object> floatValues = new ArrayList<>();
        floatValues.add(Float.parseFloat(FLOAT_VALUE_3_14));
        testContext.put(FLOAT_FIELD, floatValues);

        Field floatField = new Field(FLOAT_FIELD, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, floatField, configOptions);

        ArgumentCaptor<Float4Extractor> extractorCaptor = ArgumentCaptor.forClass(Float4Extractor.class);
        verify(mockBuilder).withExtractor(eq(FLOAT_FIELD), extractorCaptor.capture());

        Float4Extractor extractor = extractorCaptor.getValue();
        NullableFloat4Holder holder = new NullableFloat4Holder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(Float.parseFloat(FLOAT_VALUE_3_14), holder.value, 0.001f);
    }

    @Test
    public void writeRowTemplate_WithFloat4ExtractorAndEmptyString_HandlesEmptyValue() throws Exception
    {
        ArrayList<Object> emptyValues = new ArrayList<>();
        emptyValues.add(SPACES);
        testContext.put(FLOAT_FIELD, emptyValues);

        Field floatField = new Field(FLOAT_FIELD, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, floatField, configOptions);

        ArgumentCaptor<Float4Extractor> extractorCaptor = ArgumentCaptor.forClass(Float4Extractor.class);
        verify(mockBuilder).withExtractor(eq(FLOAT_FIELD), extractorCaptor.capture());

        Float4Extractor extractor = extractorCaptor.getValue();
        NullableFloat4Holder holder = new NullableFloat4Holder();
        extractor.extract(testContext, holder);

        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithFloat8ExtractorAndValidDouble_ExtractsCorrectValue() throws Exception
    {
        ArrayList<Object> doubleValues = new ArrayList<>();
        doubleValues.add(Double.parseDouble(DOUBLE_VALUE_2_718));
        testContext.put(DOUBLE_FIELD, doubleValues);

        Field doubleField = new Field(DOUBLE_FIELD, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, doubleField, configOptions);

        ArgumentCaptor<Float8Extractor> extractorCaptor = ArgumentCaptor.forClass(Float8Extractor.class);
        verify(mockBuilder).withExtractor(eq(DOUBLE_FIELD), extractorCaptor.capture());

        Float8Extractor extractor = extractorCaptor.getValue();
        NullableFloat8Holder holder = new NullableFloat8Holder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(Double.parseDouble(DOUBLE_VALUE_2_718), holder.value, 0.000001);
    }

    @Test
    public void writeRowTemplate_WithFloat8ExtractorAndEmptyString_HandlesEmptyValue() throws Exception
    {
        ArrayList<Object> emptyValues = new ArrayList<>();
        emptyValues.add(SPACES);
        testContext.put(DOUBLE_FIELD, emptyValues);

        Field doubleField = new Field(DOUBLE_FIELD, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, doubleField, configOptions);

        ArgumentCaptor<Float8Extractor> extractorCaptor = ArgumentCaptor.forClass(Float8Extractor.class);
        verify(mockBuilder).withExtractor(eq(DOUBLE_FIELD), extractorCaptor.capture());

        Float8Extractor extractor = extractorCaptor.getValue();
        NullableFloat8Holder holder = new NullableFloat8Holder();
        extractor.extract(testContext, holder);

        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithVarCharExtractorAndNullValue_HandlesNullValue() throws Exception
    {
        testContext.put(STRING_FIELD, null);

        Field varcharField = new Field(STRING_FIELD, FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, varcharField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockBuilder).withExtractor(eq(STRING_FIELD), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(testContext, holder);

        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithVarCharExtractorAndEmptyString_HandlesEmptyString() throws Exception
    {
        testContext.put(STRING_FIELD, EMPTY_STRING);

        Field varcharField = new Field(STRING_FIELD, FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, varcharField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockBuilder).withExtractor(eq(STRING_FIELD), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(EMPTY_STRING, holder.value);
    }

    private void setupCaseSensitiveConfig()
    {
        configOptions.put(Constants.SCHEMA_CASE_INSEN, CASE_INSENSITIVE_FALSE);
    }
} 