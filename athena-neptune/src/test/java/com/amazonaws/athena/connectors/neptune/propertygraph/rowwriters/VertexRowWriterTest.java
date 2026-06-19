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
import java.util.Arrays;
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
public class VertexRowWriterTest
{
    public static final String ACTIVE = "active";
    public static final String INACTIVE = "inactive";
    public static final String NULL_FIELD = "nullField";
    public static final String EMPTY_FIELD = "emptyField";
    public static final String NAME = "name";
    public static final String MULTI_FIELD = "multiField";
    public static final String EMPTY_LIST = "emptyList";
    public static final String CREATED_DATE = "createdDate";
    public static final String AGE = "age";
    public static final String BIG_NUMBER = "bigNumber";
    public static final String SCORE = "score";
    public static final String PRECISION = "precision";
    public static final String NAME_CAPS = "NAME";
    public static final String TESTFIELD = "TESTFIELD";

    // Test values
    private static final String VERTEX_123 = "vertex123";
    private static final String PERSON_LABEL = "person";
    private static final String JOHN_DOE = "John Doe";
    private static final String VALUE_1 = "value1";
    private static final String VALUE_2 = "value2";
    private static final String VALUE_3 = "value3";
    private static final String SEMICOLON_SEPARATED = ";";
    private static final String TEST_VALUE = "Test Value";
    private static final String SPACES = "   ";
    private static final String CASE_INSENSITIVE_TRUE = "true";
    private static final String CASE_INSENSITIVE_FALSE = "false";
    private static final String TESTFIELD_LOWER = "testfield";

    @Mock
    private RowWriterBuilder mockRowWriterBuilder;

    private Map<String, String> configOptions;
    private Map<String, Object> vertexContext;

    @Before
    public void setUp()
    {
        when(mockRowWriterBuilder.withExtractor(anyString(), any())).thenReturn(mockRowWriterBuilder);

        configOptions = new HashMap<>();
        vertexContext = new HashMap<>();
        vertexContext.put(T.id.toString(), VERTEX_123);
        vertexContext.put(T.label.toString(), PERSON_LABEL);
    }

    @Test
    public void writeRowTemplate_WithBitExtractorAndValidBoolean_ExtractsCorrectValue() throws Exception
    {
        putVertexList(ACTIVE, true);
        BitExtractor extractor = captureExtractorAfterWrite(boolField(ACTIVE), BitExtractor.class);
        NullableBitHolder holder = new NullableBitHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(1, holder.value);
    }

    @Test
    public void writeRowTemplate_WithBitExtractorAndValidFalse_ExtractsCorrectValue() throws Exception
    {
        putVertexList(INACTIVE, false);
        BitExtractor extractor = captureExtractorAfterWrite(boolField(INACTIVE), BitExtractor.class);
        NullableBitHolder holder = new NullableBitHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(0, holder.value);
    }

    @Test
    public void writeRowTemplate_WithBitExtractorAndNullValue_HandlesNullValue() throws Exception
    {
        vertexContext.put(NULL_FIELD, null);
        BitExtractor extractor = captureExtractorAfterWrite(boolField(NULL_FIELD), BitExtractor.class);
        NullableBitHolder holder = new NullableBitHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithBitExtractorAndEmptyString_HandlesEmptyString() throws Exception
    {
        putVertexList(EMPTY_FIELD, SPACES);
        BitExtractor extractor = captureExtractorAfterWrite(boolField(EMPTY_FIELD), BitExtractor.class);
        NullableBitHolder holder = new NullableBitHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithVarCharExtractorAndRegularField_ExtractsCorrectValue() throws Exception
    {
        putVertexList(NAME, JOHN_DOE);
        VarCharExtractor extractor = captureExtractorAfterWrite(utf8Field(NAME), VarCharExtractor.class);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(JOHN_DOE, holder.value);
    }

    @Test
    public void writeRowTemplate_WithVarCharExtractorAndSpecialIDField_HandlesSpecialIDField() throws Exception
    {
        Field idField = utf8Field(SpecialKeys.ID.toString().toLowerCase());
        VarCharExtractor extractor = captureExtractorAfterWrite(idField, VarCharExtractor.class);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(VERTEX_123, holder.value);
    }

    @Test
    public void writeRowTemplate_WithVarCharExtractorAndMultipleValues_ConcatenatesValues() throws Exception
    {
        putVertexList(MULTI_FIELD, VALUE_1, VALUE_2, VALUE_3);
        VarCharExtractor extractor = captureExtractorAfterWrite(utf8Field(MULTI_FIELD), VarCharExtractor.class);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(VALUE_1 + SEMICOLON_SEPARATED + VALUE_2 + SEMICOLON_SEPARATED + VALUE_3, holder.value);
    }

    @Test
    public void writeRowTemplate_WithVarCharExtractorAndEmptyArrayList_HandlesEmptyArrayList() throws Exception
    {
        putVertexList(EMPTY_LIST);
        VarCharExtractor extractor = captureExtractorAfterWrite(utf8Field(EMPTY_LIST), VarCharExtractor.class);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithDateMilliExtractorAndValidDate_ExtractsCorrectValue() throws Exception
    {
        Date testDate = new Date();
        putVertexList(CREATED_DATE, testDate);
        DateMilliExtractor extractor = captureExtractorAfterWrite(dateMilliField(CREATED_DATE), DateMilliExtractor.class);
        NullableDateMilliHolder holder = new NullableDateMilliHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(testDate.getTime(), holder.value);
    }

    @Test
    public void writeRowTemplate_WithIntExtractorAndValidInteger_ExtractsCorrectValue() throws Exception
    {
        putVertexList(AGE, 30);
        IntExtractor extractor = captureExtractorAfterWrite(int32Field(AGE), IntExtractor.class);
        NullableIntHolder holder = new NullableIntHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(30, holder.value);
    }

    @Test
    public void writeRowTemplate_WithBigIntExtractorAndValidLong_ExtractsCorrectValue() throws Exception
    {
        putVertexList(BIG_NUMBER, 9223372036854775807L);
        BigIntExtractor extractor = captureExtractorAfterWrite(int64Field(BIG_NUMBER), BigIntExtractor.class);
        NullableBigIntHolder holder = new NullableBigIntHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(9223372036854775807L, holder.value);
    }

    @Test
    public void writeRowTemplate_WithFloat4ExtractorAndValidFloat_ExtractsCorrectValue() throws Exception
    {
        putVertexList(SCORE, 3.14f);
        Float4Extractor extractor = captureExtractorAfterWrite(float4Field(SCORE), Float4Extractor.class);
        NullableFloat4Holder holder = new NullableFloat4Holder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(3.14f, holder.value, 0.001f);
    }

    @Test
    public void writeRowTemplate_WithFloat8ExtractorAndValidDouble_ExtractsCorrectValue() throws Exception
    {
        putVertexList(PRECISION, 2.718281828);
        Float8Extractor extractor = captureExtractorAfterWrite(float8Field(PRECISION), Float8Extractor.class);
        NullableFloat8Holder holder = new NullableFloat8Holder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(2.718281828, holder.value, 0.000001);
    }

    @Test
    public void writeRowTemplate_WithContextAsMapAndCaseInsensitive_HandlesCaseInsensitive() throws Exception
    {
        setupSchemaCaseInsensitive(CASE_INSENSITIVE_TRUE);
        putVertexList(NAME, JOHN_DOE);
        VarCharExtractor extractor = captureExtractorAfterWrite(utf8Field(NAME_CAPS), VarCharExtractor.class);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(JOHN_DOE, holder.value);
    }

    @Test
    public void writeRowTemplate_WithContextAsMapAndCaseSensitive_HandlesCaseSensitive() throws Exception
    {
        setupSchemaCaseInsensitive(CASE_INSENSITIVE_FALSE);
        putVertexList(NAME, JOHN_DOE);
        VarCharExtractor extractor = captureExtractorAfterWrite(utf8Field(NAME_CAPS), VarCharExtractor.class);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithContextAsMapAndDefaultCaseInsensitive_HandlesDefaultCaseInsensitive() throws Exception
    {
        putVertexList(TESTFIELD_LOWER, TEST_VALUE);
        VarCharExtractor extractor = captureExtractorAfterWrite(utf8Field(TESTFIELD), VarCharExtractor.class);
        NullableVarCharHolder holder = new NullableVarCharHolder();
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(TEST_VALUE, holder.value);
    }

    @Test(expected = NullPointerException.class)
    public void writeRowTemplate_WithNullRowWriterBuilder_ThrowsNullPointerException() throws Exception
    {
        VertexRowWriter.writeRowTemplate(null, utf8Field(NAME), configOptions);
    }

    @Test(expected = NullPointerException.class)
    public void writeRowTemplate_WithNullField_ThrowsNullPointerException() throws Exception
    {
        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, null, configOptions);
    }

    @Test(expected = NullPointerException.class)
    public void writeRowTemplate_WithNullConfigOptions_ThrowsNullPointerException() throws Exception
    {
        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, utf8Field(NAME), null);
    }

    private <E extends Extractor> E captureExtractorAfterWrite(Field field, Class<E> extractorType)
    {
        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, field, configOptions);
        ArgumentCaptor<E> captor = ArgumentCaptor.forClass(extractorType);
        verify(mockRowWriterBuilder).withExtractor(eq(field.getName()), captor.capture());
        return captor.getValue();
    }

    private static Field boolField(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Bool()), null);
    }

    private static Field utf8Field(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Utf8()), null);
    }

    private static Field dateMilliField(String name)
    {
        return new Field(name, FieldType.nullable(Types.MinorType.DATEMILLI.getType()), null);
    }

    private static Field int32Field(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Int(32, true)), null);
    }

    private static Field int64Field(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.Int(64, true)), null);
    }

    private static Field float4Field(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
    }

    private static Field float8Field(String name)
    {
        return new Field(name, FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null);
    }

    private void setupSchemaCaseInsensitive(String caseInsensitiveValue)
    {
        configOptions.put(Constants.SCHEMA_CASE_INSEN, caseInsensitiveValue);
    }

    private void putVertexList(String fieldName, Object... elements)
    {
        vertexContext.put(fieldName, new ArrayList<>(Arrays.asList(elements)));
    }
}
