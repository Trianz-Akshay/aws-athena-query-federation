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
    private static final String ID_FIELD = "id";
    private static final String TESTFIELD_LOWER = "testfield";
    @Mock
    private RowWriterBuilder mockRowWriterBuilder;

    private Map<String, String> configOptions;
    private Map<String, Object> vertexContext;

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
    }

    @Test
    public void writeRowTemplate_WithBitExtractorAndValidBoolean_ExtractsCorrectValue() throws Exception
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
    public void writeRowTemplate_WithBitExtractorAndValidFalse_ExtractsCorrectValue() throws Exception
    {
        addToVertexContext(INACTIVE, false);

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
    public void writeRowTemplate_WithBitExtractorAndNullValue_HandlesNullValue() throws Exception
    {
        vertexContext.put(NULL_FIELD, null);

        Field bitField = new Field(NULL_FIELD, FieldType.nullable(new ArrowType.Bool()), null);

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, bitField, configOptions);

        ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(NULL_FIELD), extractorCaptor.capture());

        BitExtractor extractor = extractorCaptor.getValue();
        NullableBitHolder holder = new NullableBitHolder();

        extractor.extract(vertexContext, holder);
        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithBitExtractorAndEmptyString_HandlesEmptyString() throws Exception
    {
        ArrayList<Object> emptyValues = new ArrayList<>();
        emptyValues.add(SPACES);
        vertexContext.put(EMPTY_FIELD, emptyValues);

        Field bitField = new Field(EMPTY_FIELD, FieldType.nullable(new ArrowType.Bool()), null);

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, bitField, configOptions);

        ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(EMPTY_FIELD), extractorCaptor.capture());

        BitExtractor extractor = extractorCaptor.getValue();
        NullableBitHolder holder = new NullableBitHolder();

        extractor.extract(vertexContext, holder);
        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithVarCharExtractorAndRegularField_ExtractsCorrectValue() throws Exception
    {
        ArrayList<Object> nameValues = new ArrayList<>();
        nameValues.add(JOHN_DOE);
        vertexContext.put(NAME, nameValues);

        Field varcharField = new Field(NAME, FieldType.nullable(new ArrowType.Utf8()), null);

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(NAME), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        NullableVarCharHolder holder = new NullableVarCharHolder();

        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(JOHN_DOE, holder.value);
    }

    @Test
    public void writeRowTemplate_WithVarCharExtractorAndSpecialIDField_HandlesSpecialIDField() throws Exception
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
    public void writeRowTemplate_WithVarCharExtractorAndMultipleValues_ConcatenatesValues() throws Exception
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
    public void writeRowTemplate_WithVarCharExtractorAndEmptyArrayList_HandlesEmptyArrayList() throws Exception
    {
        vertexContext.put(EMPTY_LIST, new ArrayList<>());

        Field varcharField = new Field(EMPTY_LIST, FieldType.nullable(new ArrowType.Utf8()), null);

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(EMPTY_LIST), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        NullableVarCharHolder holder = new NullableVarCharHolder();

        extractor.extract(vertexContext, holder);
        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithDateMilliExtractorAndValidDate_ExtractsCorrectValue() throws Exception
    {
        Date testDate = new Date();
        ArrayList<Object> dateValues = new ArrayList<>();
        dateValues.add(testDate);
        vertexContext.put(CREATED_DATE, dateValues);

        Field dateField = new Field(CREATED_DATE, FieldType.nullable(org.apache.arrow.vector.types.Types.MinorType.DATEMILLI.getType()), null);

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
    public void writeRowTemplate_WithIntExtractorAndValidInteger_ExtractsCorrectValue() throws Exception
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
    public void writeRowTemplate_WithBigIntExtractorAndValidLong_ExtractsCorrectValue() throws Exception
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
    public void writeRowTemplate_WithFloat4ExtractorAndValidFloat_ExtractsCorrectValue() throws Exception
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
    public void writeRowTemplate_WithFloat8ExtractorAndValidDouble_ExtractsCorrectValue() throws Exception
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
    public void writeRowTemplate_WithContextAsMapAndCaseInsensitive_HandlesCaseInsensitive() throws Exception
    {
        setupCaseInsensitiveConfig();
        addToVertexContext(NAME, JOHN_DOE);

        Field varcharField = new Field(NAME_CAPS, FieldType.nullable(new ArrowType.Utf8()), null);

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(NAME_CAPS), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        NullableVarCharHolder holder = new NullableVarCharHolder();

        // Should find "name" field despite case difference
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(JOHN_DOE, holder.value);
    }

    @Test
    public void writeRowTemplate_WithContextAsMapAndCaseSensitive_HandlesCaseSensitive() throws Exception
    {
        setupCaseSensitiveConfig();
        addToVertexContext(NAME, JOHN_DOE);

        Field varcharField = new Field(NAME_CAPS, FieldType.nullable(new ArrowType.Utf8()), null);

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(NAME_CAPS), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        NullableVarCharHolder holder = new NullableVarCharHolder();

        // Should NOT find "name" field due to case difference
        extractor.extract(vertexContext, holder);
        assertEquals(0, holder.isSet);
    }

    @Test
    public void writeRowTemplate_WithContextAsMapAndDefaultCaseInsensitive_HandlesDefaultCaseInsensitive() throws Exception
    {
        // When no configuration is provided, should default to case insensitive
        ArrayList<Object> nameValues = new ArrayList<>();
        nameValues.add(TEST_VALUE);
        vertexContext.put(TESTFIELD_LOWER, nameValues);

        Field varcharField = new Field(TESTFIELD, FieldType.nullable(new ArrowType.Utf8()), null);

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, varcharField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(TESTFIELD), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        NullableVarCharHolder holder = new NullableVarCharHolder();

        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(TEST_VALUE, holder.value);
    }

    @Test
    public void writeRowTemplate_WithContextAsMapAndIDFieldTransformation_HandlesIDFieldTransformation() throws Exception
    {
        // Test that T.id is transformed to SpecialKeys.ID
        Field idField = new Field(SpecialKeys.ID.toString().toLowerCase(), FieldType.nullable(new ArrowType.Utf8()), null);

        VertexRowWriter.writeRowTemplate(mockRowWriterBuilder, idField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(ID_FIELD), extractorCaptor.capture());

        VarCharExtractor extractor = extractorCaptor.getValue();
        NullableVarCharHolder holder = new NullableVarCharHolder();

        // After contextAsMap transformation, T.id should become SpecialKeys.ID
        extractor.extract(vertexContext, holder);
        assertEquals(1, holder.isSet);
        assertEquals(VERTEX_123, holder.value);
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
}