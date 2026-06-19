/*-
 * #%L
 * athena-neptune
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
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
import java.util.Arrays;
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
    public void writeRowTemplate_WithBitField_CreatesBitExtractor()
    {
        writeTemplateAndVerifyExtractorRegistered(fieldBool(BOOL_FIELD), BOOL_FIELD);
    }

    @Test
    public void writeRowTemplate_WithVarCharFieldAndStringType_CreatesVarCharExtractor()
    {
        writeTemplateAndVerifyExtractorRegistered(fieldUtf8(STRING_FIELD), STRING_FIELD);
    }

    @Test
    public void writeRowTemplate_WithDateMilliFieldAndDateType_CreatesDateMilliExtractor()
    {
        writeTemplateAndVerifyExtractorRegistered(fieldDateMilli(DATE_FIELD), DATE_FIELD);
    }

    @Test
    public void writeRowTemplate_WithIntFieldAndIntegerType_CreatesIntExtractor()
    {
        writeTemplateAndVerifyExtractorRegistered(fieldInt32(INT_FIELD), INT_FIELD);
    }

    @Test
    public void writeRowTemplate_WithBigIntFieldAndLongType_CreatesBigIntExtractor()
    {
        writeTemplateAndVerifyExtractorRegistered(fieldInt64(LONG_FIELD), LONG_FIELD);
    }

    @Test
    public void writeRowTemplate_WithFloat4FieldAndFloatType_CreatesFloat4Extractor()
    {
        writeTemplateAndVerifyExtractorRegistered(fieldFloat4(FLOAT_FIELD), FLOAT_FIELD);
    }

    @Test
    public void writeRowTemplate_WithFloat8FieldAndDoubleType_CreatesFloat8Extractor()
    {
        writeTemplateAndVerifyExtractorRegistered(fieldFloat8(DOUBLE_FIELD), DOUBLE_FIELD);
    }

    @Test
    public void writeRowTemplate_WithIdField_CreatesVarCharExtractor()
    {
        writeTemplateAndVerifyExtractorRegistered(fieldUtf8(ID), ID);
    }

    @Test
    public void writeRowTemplate_WithNullValues_CreatesVarCharExtractor()
    {
        writeTemplateAndVerifyExtractorRegistered(fieldUtf8(NULL_FIELD), NULL_FIELD);
    }

    @Test
    public void writeRowTemplate_WithEmptyValues_CreatesVarCharExtractor()
    {
        writeTemplateAndVerifyExtractorRegistered(fieldUtf8(EMPTY_FIELD), EMPTY_FIELD);
    }

    @Test
    public void writeRowTemplate_WithCaseInsensitiveDisabledAndStringType_CreatesVarCharExtractor()
    {
        setupCaseSensitiveConfig();
        writeTemplateAndVerifyExtractorRegistered(fieldUtf8(MIXED_CASE_FIELD), MIXED_CASE_FIELD);
    }

    @Test(expected = IllegalAccessException.class)
    public void constructor_WithPrivateAccess_ThrowsIllegalAccessException() throws Exception
    {
        CustomSchemaRowWriter.class.getDeclaredConstructor().newInstance();
    }

    @Test
    public void writeRowTemplate_WithBitExtractorAndBooleanInContext_ExtractsCorrectValue() throws Exception
    {
        // BIT branch: fieldValue is Boolean (not ArrayList)
        testContext.put(BOOL_FIELD, Boolean.TRUE);

        BitExtractor extractor = captureExtractorAfterWrite(fieldBool(BOOL_FIELD), BitExtractor.class, BOOL_FIELD);
        NullableBitHolder holder = new NullableBitHolder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(1, holder.value);
    }

    @Test
    public void writeRowTemplate_WithBitExtractorAndArrayListInContext_ExtractsCorrectValue() throws Exception
    {
        // BIT branch: fieldValue instanceof ArrayList — Gremlin valueMap often wraps in list
        putListContext(BOOL_FIELD, true);

        BitExtractor extractor = captureExtractorAfterWrite(fieldBool(BOOL_FIELD), BitExtractor.class, BOOL_FIELD);
        NullableBitHolder holder = new NullableBitHolder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(1, holder.value);
    }

    @Test
    public void writeRowTemplate_WithBitExtractorAndEmptyString_HandlesEmptyValue() throws Exception
    {
        putListContext(BOOL_FIELD, SPACES);

        BitExtractor extractor = captureExtractorAfterWrite(fieldBool(BOOL_FIELD), BitExtractor.class, BOOL_FIELD);
        NullableBitHolder holder = new NullableBitHolder();
        extractor.extract(testContext, holder);

        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithVarCharExtractorAndSingleValue_ExtractsCorrectValue() throws Exception
    {
        putListContext(STRING_FIELD, TEST_VALUE);

        VarCharExtractor extractor = captureExtractorAfterWrite(fieldUtf8(STRING_FIELD), VarCharExtractor.class, STRING_FIELD);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(TEST_VALUE, holder.value);
    }

    @Test
    public void writeRowTemplate_WithVarCharExtractorAndMultipleValues_ConcatenatesValues() throws Exception
    {
        putListContext(STRING_FIELD, VALUE_1, VALUE_2, VALUE_3);

        VarCharExtractor extractor = captureExtractorAfterWrite(fieldUtf8(STRING_FIELD), VarCharExtractor.class, STRING_FIELD);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(VALUE_1 + SEMICOLON_SEPARATED + VALUE_2 + SEMICOLON_SEPARATED + VALUE_3, holder.value);
    }

    @Test
    public void writeRowTemplate_WithVarCharExtractorAndOtherType_ConvertsToString() throws Exception
    {
        testContext.put(STRING_FIELD, 123);

        VarCharExtractor extractor = captureExtractorAfterWrite(fieldUtf8(STRING_FIELD), VarCharExtractor.class, STRING_FIELD);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(STRING_123, holder.value);
    }

    @Test
    public void writeRowTemplate_WithDateMilliExtractorAndValidDate_ExtractsCorrectValue() throws Exception
    {
        Date testDate = new Date();
        putListContext(DATE_FIELD, testDate);

        DateMilliExtractor extractor = captureExtractorAfterWrite(fieldDateMilli(DATE_FIELD), DateMilliExtractor.class, DATE_FIELD);
        NullableDateMilliHolder holder = new NullableDateMilliHolder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(testDate.getTime(), holder.value);
    }

    @Test
    public void writeRowTemplate_WithDateMilliExtractorAndEmptyString_HandlesEmptyValue() throws Exception
    {
        putListContext(DATE_FIELD, SPACES);

        DateMilliExtractor extractor = captureExtractorAfterWrite(fieldDateMilli(DATE_FIELD), DateMilliExtractor.class, DATE_FIELD);
        NullableDateMilliHolder holder = new NullableDateMilliHolder();
        extractor.extract(testContext, holder);

        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithIntExtractorAndValidInteger_ExtractsCorrectValue() throws Exception
    {
        putListContext(INT_FIELD, 42);

        IntExtractor extractor = captureExtractorAfterWrite(fieldInt32(INT_FIELD), IntExtractor.class, INT_FIELD);
        NullableIntHolder holder = new NullableIntHolder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(42, holder.value);
    }

    @Test
    public void writeRowTemplate_WithIntExtractorAndEmptyString_HandlesEmptyValue() throws Exception
    {
        putListContext(INT_FIELD, SPACES);

        IntExtractor extractor = captureExtractorAfterWrite(fieldInt32(INT_FIELD), IntExtractor.class, INT_FIELD);
        NullableIntHolder holder = new NullableIntHolder();
        extractor.extract(testContext, holder);

        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithBigIntExtractorAndValidLong_ExtractsCorrectValue() throws Exception
    {
        putListContext(LONG_FIELD, Long.parseLong(BIG_INT_VALUE));

        BigIntExtractor extractor = captureExtractorAfterWrite(fieldInt64(LONG_FIELD), BigIntExtractor.class, LONG_FIELD);
        NullableBigIntHolder holder = new NullableBigIntHolder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(Long.parseLong(BIG_INT_VALUE), holder.value);
    }

    @Test
    public void writeRowTemplate_WithBigIntExtractorAndEmptyString_HandlesEmptyValue() throws Exception
    {
        putListContext(LONG_FIELD, SPACES);

        BigIntExtractor extractor = captureExtractorAfterWrite(fieldInt64(LONG_FIELD), BigIntExtractor.class, LONG_FIELD);
        NullableBigIntHolder holder = new NullableBigIntHolder();
        extractor.extract(testContext, holder);

        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithFloat4ExtractorAndValidFloat_ExtractsCorrectValue() throws Exception
    {
        putListContext(FLOAT_FIELD, Float.parseFloat(FLOAT_VALUE_3_14));

        Float4Extractor extractor = captureExtractorAfterWrite(fieldFloat4(FLOAT_FIELD), Float4Extractor.class, FLOAT_FIELD);
        NullableFloat4Holder holder = new NullableFloat4Holder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(Float.parseFloat(FLOAT_VALUE_3_14), holder.value, 0.001f);
    }

    @Test
    public void writeRowTemplate_WithFloat4ExtractorAndEmptyString_HandlesEmptyValue() throws Exception
    {
        putListContext(FLOAT_FIELD, SPACES);

        Float4Extractor extractor = captureExtractorAfterWrite(fieldFloat4(FLOAT_FIELD), Float4Extractor.class, FLOAT_FIELD);
        NullableFloat4Holder holder = new NullableFloat4Holder();
        extractor.extract(testContext, holder);

        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithFloat8ExtractorAndValidDouble_ExtractsCorrectValue() throws Exception
    {
        putListContext(DOUBLE_FIELD, Double.parseDouble(DOUBLE_VALUE_2_718));

        Float8Extractor extractor = captureExtractorAfterWrite(fieldFloat8(DOUBLE_FIELD), Float8Extractor.class, DOUBLE_FIELD);
        NullableFloat8Holder holder = new NullableFloat8Holder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(Double.parseDouble(DOUBLE_VALUE_2_718), holder.value, 0.000001);
    }

    @Test
    public void writeRowTemplate_WithFloat8ExtractorAndEmptyString_HandlesEmptyValue() throws Exception
    {
        putListContext(DOUBLE_FIELD, SPACES);

        Float8Extractor extractor = captureExtractorAfterWrite(fieldFloat8(DOUBLE_FIELD), Float8Extractor.class, DOUBLE_FIELD);
        NullableFloat8Holder holder = new NullableFloat8Holder();
        extractor.extract(testContext, holder);

        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithVarCharExtractorAndNullValue_HandlesNullValue() throws Exception
    {
        testContext.put(STRING_FIELD, null);

        VarCharExtractor extractor = captureExtractorAfterWrite(fieldUtf8(STRING_FIELD), VarCharExtractor.class, STRING_FIELD);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(testContext, holder);

        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithVarCharExtractorAndEmptyString_HandlesEmptyString() throws Exception
    {
        testContext.put(STRING_FIELD, EMPTY_STRING);

        VarCharExtractor extractor = captureExtractorAfterWrite(fieldUtf8(STRING_FIELD), VarCharExtractor.class, STRING_FIELD);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(testContext, holder);

        assertEquals(1, holder.isSet);
        assertEquals(EMPTY_STRING, holder.value);
    }

    private void setupCaseSensitiveConfig()
    {
        configOptions.put(Constants.SCHEMA_CASE_INSEN, CASE_INSENSITIVE_FALSE);
    }

    @Test
    public void writeRowTemplate_WithDefaultCaseInsensitiveAndUtf8Type_ResolvesMixedCaseKey() throws Exception
    {
        configOptions.clear();
        final String mixedCase = "MIXEDCASE";
        final String mixedCaseLower = "mixedcase";
        final String mixedCaseValue = "Mixed Case Value";
        testContext.put(mixedCaseLower, mixedCaseValue);

        VarCharExtractor extractor = captureExtractorAfterWrite(fieldUtf8(mixedCase), VarCharExtractor.class, mixedCase);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(testContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(mixedCaseValue, holder.value);
    }

    @Test
    public void writeRowTemplate_WithDateMilliExtractorAndEmptyStringInContext_HandlesUnset() throws Exception
    {
        final String emptyDateField = "emptyDate";
        testContext.put(emptyDateField, EMPTY_STRING);

        DateMilliExtractor extractor = captureExtractorAfterWrite(fieldDateMilli(emptyDateField), DateMilliExtractor.class, emptyDateField);
        NullableDateMilliHolder holder = new NullableDateMilliHolder();
        extractor.extract(testContext, holder);
        assertEquals(0, holder.isSet);
    }

    private void writeRowTemplate(Field field)
    {
        CustomSchemaRowWriter.writeRowTemplate(mockBuilder, field, configOptions);
    }

    private void writeTemplateAndVerifyExtractorRegistered(Field field, String expectedFieldName)
    {
        writeRowTemplate(field);
        verify(mockBuilder).withExtractor(eq(expectedFieldName), any());
    }

    private Field fieldBool(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Bool()), Collections.emptyList());
    }

    private Field fieldUtf8(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Utf8()), Collections.emptyList());
    }

    private Field fieldDateMilli(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), Collections.emptyList());
    }

    private Field fieldInt32(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), Collections.emptyList());
    }

    private Field fieldInt64(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Int(64, true)), Collections.emptyList());
    }

    private Field fieldFloat4(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), Collections.emptyList());
    }

    private Field fieldFloat8(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), Collections.emptyList());
    }

    private void putListContext(String fieldName, Object... elements)
    {
        testContext.put(fieldName, new ArrayList<>(Arrays.asList(elements)));
    }

    private <E extends Extractor> E captureExtractorAfterWrite(Field field, Class<E> extractorType, String verifyFieldName)
    {
        writeRowTemplate(field);
        ArgumentCaptor<E> captor = ArgumentCaptor.forClass(extractorType);
        verify(mockBuilder).withExtractor(eq(verifyFieldName), captor.capture());
        return captor.getValue();
    }
}
