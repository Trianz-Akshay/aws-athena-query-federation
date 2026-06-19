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

import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter.RowWriterBuilder;
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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class EdgeRowWriterTest
{
    public static final String TEST_FIELD = "testField";

    // Field names for edge-context extractor tests (moved from RowWritersComprehensiveTest)
    private static final String IS_ACTIVE = "isActive";
    private static final String DESCRIPTION = "description";
    private static final String CREATED_AT = "createdAt";
    private static final String SINCE = "since";
    private static final String WEIGHT = "weight";
    private static final String BIG_VALUE = "bigValue";
    private static final String FLOAT_VALUE = "floatValue";
    private static final String EMPTY_FIELD = "emptyField";
    private static final String TESTFIELD = "TESTFIELD";
    private static final String TESTFIELD_LOWER = "testfield";
    private static final String FRIENDSHIP = "friendship";
    private static final String TEST_VALUE = "Test Value";
    private static final String SPACES = "   ";

    // Configuration values
    private static final String CASE_INSENSITIVE_TRUE = "true";
    private static final String CASE_INSENSITIVE_FALSE = "false";

    @Mock
    private RowWriterBuilder mockRowWriterBuilder;

    @Mock
    private Field mockField;

    private Map<String, String> configOptions;
    private Map<String, Object> edgeContext;

    @Before
    public void setUp()
    {
        when(mockRowWriterBuilder.withExtractor(anyString(), any())).thenReturn(mockRowWriterBuilder);

        when(mockField.getName()).thenReturn(TEST_FIELD);

        configOptions = new HashMap<>();

        edgeContext = new HashMap<>();
        edgeContext.put(T.id.toString(), "edge456");
        edgeContext.put(T.label.toString(), "knows");
    }

    @Test
    public void writeRowTemplate_WithCaseInsensitiveConfigAndBoolType_CreatesBitExtractor() throws Exception
    {
        mockFieldArrowType(ArrowType.Bool.INSTANCE);
        setupSchemaCaseInsensitive(CASE_INSENSITIVE_TRUE);

        BitExtractor capturedExtractor = captureExtractorAfterWrite(mockField, BitExtractor.class, TEST_FIELD);
        assertNotNull("BitExtractor should not be null", capturedExtractor);

        Map<String, Object> context = new HashMap<>();
        context.put(TEST_FIELD, Boolean.TRUE);
        NullableBitHolder holder = new NullableBitHolder();
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals(1, holder.value);

        context.put(TEST_FIELD, "false");
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals(0, holder.value);
    }

    @Test
    public void writeRowTemplate_WithCaseInsensitiveConfigAndUtf8Type_CreatesVarCharExtractor() throws Exception
    {
        mockFieldArrowType(ArrowType.Utf8.INSTANCE);
        setupSchemaCaseInsensitive(CASE_INSENSITIVE_TRUE);

        VarCharExtractor capturedExtractor = captureExtractorAfterWrite(mockField, VarCharExtractor.class, TEST_FIELD);
        assertNotNull("VarCharExtractor should not be null", capturedExtractor);

        Map<String, Object> context = new HashMap<>();
        context.put(TEST_FIELD, "edge-value");
        NullableVarCharHolder holder = new NullableVarCharHolder();
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals("edge-value", holder.value);

        context.put(TEST_FIELD, 12345);
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals("12345", holder.value);
    }

    @Test
    public void writeRowTemplate_WithCaseInsensitiveDisabledAndUtf8Type_CreatesVarCharExtractor() throws Exception
    {
        mockFieldArrowType(ArrowType.Utf8.INSTANCE);
        setupSchemaCaseInsensitive(CASE_INSENSITIVE_FALSE);

        VarCharExtractor capturedExtractor = captureExtractorAfterWrite(mockField, VarCharExtractor.class, TEST_FIELD);
        assertNotNull("VarCharExtractor should not be null", capturedExtractor);

        Map<String, Object> context = new HashMap<>();
        context.put(TEST_FIELD, "case-sensitive-key-only");
        NullableVarCharHolder holder = new NullableVarCharHolder();
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals("case-sensitive-key-only", holder.value);
    }

    @Test
    public void writeRowTemplate_WithNullCaseInsensitiveConfigAndUtf8Type_CreatesVarCharExtractor() throws Exception
    {
        mockFieldArrowType(ArrowType.Utf8.INSTANCE);

        VarCharExtractor capturedExtractor = captureExtractorAfterWrite(mockField, VarCharExtractor.class, TEST_FIELD);
        assertNotNull("VarCharExtractor should not be null", capturedExtractor);

        Map<String, Object> context = new HashMap<>();
        context.put(TEST_FIELD, "default-config");
        NullableVarCharHolder holder = new NullableVarCharHolder();
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals("default-config", holder.value);
    }

    @Test
    public void writeRowTemplate_WithEmptyConfigOptionsAndUtf8Type_CreatesVarCharExtractor() throws Exception
    {
        mockFieldArrowType(ArrowType.Utf8.INSTANCE);

        VarCharExtractor capturedExtractor = captureExtractorAfterWrite(mockField, VarCharExtractor.class, TEST_FIELD);
        assertNotNull("VarCharExtractor should not be null", capturedExtractor);

        Map<String, Object> context = new HashMap<>();
        context.put(TEST_FIELD, "empty-config-options");
        NullableVarCharHolder holder = new NullableVarCharHolder();
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals("empty-config-options", holder.value);
    }

    @Test(expected = NullPointerException.class)
    public void writeRowTemplate_WithNullConfigOptionsAndUtf8Type_ThrowsNullPointerException()
    {
        mockFieldArrowType(ArrowType.Utf8.INSTANCE);

        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, mockField, null);
    }

    @Test(expected = NullPointerException.class)
    public void writeRowTemplate_WithNullRowWriterBuilderAndUtf8Type_ThrowsNullPointerException()
    {
        mockFieldArrowType(ArrowType.Utf8.INSTANCE);

        EdgeRowWriter.writeRowTemplate(null, mockField, configOptions);
    }

    @Test(expected = NullPointerException.class)
    public void writeRowTemplate_WithNullField_ThrowsNullPointerException()
    {
        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, null, configOptions);
    }

    @Test
    public void writeRowTemplate_WithCaseInsensitiveConfigAndBigIntType_CreatesBigIntExtractor() throws Exception
    {
        mockFieldArrowType(new ArrowType.Int(64, true));
        setupSchemaCaseInsensitive(CASE_INSENSITIVE_TRUE);

        BigIntExtractor capturedExtractor = captureExtractorAfterWrite(mockField, BigIntExtractor.class, TEST_FIELD);
        assertNotNull("BigIntExtractor should not be null", capturedExtractor);

        Map<String, Object> context = new HashMap<>();
        context.put(TEST_FIELD, "9876543210");
        NullableBigIntHolder holder = new NullableBigIntHolder();
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals(9876543210L, holder.value);

        context.put(TEST_FIELD, 42L);
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals(42L, holder.value);
    }

    @Test
    public void writeRowTemplate_WithCaseInsensitiveConfigAndFloat4Type_CreatesFloat4Extractor() throws Exception
    {
        mockFieldArrowType(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        setupSchemaCaseInsensitive(CASE_INSENSITIVE_TRUE);

        Float4Extractor capturedExtractor = captureExtractorAfterWrite(mockField, Float4Extractor.class, TEST_FIELD);
        assertNotNull("Float4Extractor should not be null", capturedExtractor);

        Map<String, Object> context = new HashMap<>();
        context.put(TEST_FIELD, "2.5");
        NullableFloat4Holder holder = new NullableFloat4Holder();
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals(2.5f, holder.value, 0.0001f);

        context.put(TEST_FIELD, 1.25f);
        capturedExtractor.extract(context, holder);
        assertEquals(1, holder.isSet);
        assertEquals(1.25f, holder.value, 0.0001f);
    }

    // --- Edge context + real Field tests (from RowWritersComprehensiveTest) ---

    @Test
    public void writeRowTemplate_WithEdgeContextAndBoolType_ExtractsValidBoolean() throws Exception
    {
        edgeContext.put(IS_ACTIVE, true);

        BitExtractor extractor = captureExtractorAfterWrite(edgeFieldBool(IS_ACTIVE), BitExtractor.class, IS_ACTIVE);
        NullableBitHolder holder = new NullableBitHolder();
        extractor.extract(edgeContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(1, holder.value);
    }

    @Test
    public void writeRowTemplate_WithEdgeContextAndUtf8Type_ExtractsValidString() throws Exception
    {
        edgeContext.put(DESCRIPTION, FRIENDSHIP);

        VarCharExtractor extractor = captureExtractorAfterWrite(edgeFieldUtf8(DESCRIPTION), VarCharExtractor.class, DESCRIPTION);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(edgeContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(FRIENDSHIP, holder.value);
    }

    @Test
    public void writeRowTemplate_WithEdgeContextAndDateMilliType_ExtractsValidDate() throws Exception
    {
        Date testDate = new Date();
        edgeContext.put(CREATED_AT, testDate);

        DateMilliExtractor extractor = captureExtractorAfterWrite(edgeFieldDateMilli(CREATED_AT), DateMilliExtractor.class, CREATED_AT);
        NullableDateMilliHolder holder = new NullableDateMilliHolder();
        extractor.extract(edgeContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(testDate.getTime(), holder.value);
    }

    @Test
    public void writeRowTemplate_WithEdgeContextAndIntType_ExtractsValidInteger() throws Exception
    {
        edgeContext.put(SINCE, 2020);

        IntExtractor extractor = captureExtractorAfterWrite(edgeFieldInt32(SINCE), IntExtractor.class, SINCE);
        NullableIntHolder holder = new NullableIntHolder();
        extractor.extract(edgeContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(2020, holder.value);
    }

    @Test
    public void writeRowTemplate_WithEdgeContextAndFloat8Type_ExtractsValidDouble() throws Exception
    {
        edgeContext.put(WEIGHT, 0.8);

        Float8Extractor extractor = captureExtractorAfterWrite(edgeFieldFloat8(WEIGHT), Float8Extractor.class, WEIGHT);
        NullableFloat8Holder holder = new NullableFloat8Holder();
        extractor.extract(edgeContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(0.8, holder.value, 0.001);
    }

    @Test
    public void writeRowTemplate_WithEdgeContextAndBigIntType_ExtractsValidLong() throws Exception
    {
        edgeContext.put(BIG_VALUE, 1234567890123456789L);

        BigIntExtractor extractor = captureExtractorAfterWrite(edgeFieldInt64(BIG_VALUE), BigIntExtractor.class, BIG_VALUE);
        NullableBigIntHolder holder = new NullableBigIntHolder();
        extractor.extract(edgeContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(1234567890123456789L, holder.value);
    }

    @Test
    public void writeRowTemplate_WithEdgeContextAndFloat4Type_ExtractsValidFloat() throws Exception
    {
        edgeContext.put(FLOAT_VALUE, 2.5f);

        Float4Extractor extractor = captureExtractorAfterWrite(edgeFieldFloat4(FLOAT_VALUE), Float4Extractor.class, FLOAT_VALUE);
        NullableFloat4Holder holder = new NullableFloat4Holder();
        extractor.extract(edgeContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(2.5f, holder.value, 0.001f);
    }

    @Test
    public void writeRowTemplate_WithCaseSensitiveConfigAndUtf8Type_DoesNotMatchDifferentCaseKey() throws Exception
    {
        setupSchemaCaseInsensitive(CASE_INSENSITIVE_FALSE);
        edgeContext.put(TESTFIELD_LOWER, TEST_VALUE);

        VarCharExtractor extractor = captureExtractorAfterWrite(edgeFieldUtf8(TESTFIELD), VarCharExtractor.class, TESTFIELD);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(edgeContext, holder);
        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithEdgeContextAndSpacesInBitField_HandlesUnset() throws Exception
    {
        edgeContext.put(EMPTY_FIELD, SPACES);

        BitExtractor extractor = captureExtractorAfterWrite(edgeFieldBool(EMPTY_FIELD), BitExtractor.class, EMPTY_FIELD);
        NullableBitHolder holder = new NullableBitHolder();
        extractor.extract(edgeContext, holder);
        assertEquals(0, holder.isSet);
    }

    private void setupSchemaCaseInsensitive(String caseInsensitiveValue)
    {
        configOptions.put(Constants.SCHEMA_CASE_INSEN, caseInsensitiveValue);
    }

    private void mockFieldArrowType(ArrowType arrowType)
    {
        when(mockField.getType()).thenReturn(arrowType);
    }

    private <E extends Extractor> E captureExtractorAfterWrite(Field field, Class<E> extractorType, String registeredFieldName)
    {
        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, field, configOptions);
        ArgumentCaptor<E> captor = ArgumentCaptor.forClass(extractorType);
        verify(mockRowWriterBuilder).withExtractor(eq(registeredFieldName), captor.capture());
        return captor.getValue();
    }

    private Field edgeFieldBool(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Bool()), null);
    }

    private Field edgeFieldUtf8(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Utf8()), null);
    }

    private Field edgeFieldDateMilli(String name)
    {
        return new Field(name, FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);
    }

    private Field edgeFieldInt32(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), null);
    }

    private Field edgeFieldInt64(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Int(64, true)), null);
    }

    private Field edgeFieldFloat4(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
    }

    private Field edgeFieldFloat8(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
    }
}
