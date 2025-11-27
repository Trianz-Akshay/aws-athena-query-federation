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
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float4Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connectors.neptune.Constants;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EdgeRowWriterTest
{
    public static final String TEST_FIELD = "testField";
    
    // Configuration values
    private static final String CASE_INSENSITIVE_TRUE = "true";
    private static final String CASE_INSENSITIVE_FALSE = "false";
    
    @Mock
    private RowWriterBuilder mockRowWriterBuilder;

    @Mock
    private Field mockField;

    private Map<String, String> configOptions;

    @Before
    public void setUp()
    {
        MockitoAnnotations.openMocks(this);

        // Common setup for all tests
        when(mockField.getName()).thenReturn(TEST_FIELD);

        // Initialize default config options
        configOptions = new HashMap<>();
    }

    @Test
    public void writeRowTemplate_WithBitField_CreatesBitExtractor()
    {
        ArrowType arrowType = ArrowType.Bool.INSTANCE;
        when(mockField.getType()).thenReturn(arrowType);

        setupCaseInsensitiveConfig();

        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, mockField, configOptions);

        ArgumentCaptor<BitExtractor> extractorCaptor = ArgumentCaptor.forClass(BitExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(TEST_FIELD), extractorCaptor.capture());

        BitExtractor capturedExtractor = extractorCaptor.getValue();
        assertNotNull("BitExtractor should not be null", capturedExtractor);
    }

    @Test
    public void writeRowTemplate_WithVarcharField_CreatesVarCharExtractor()
    {
        ArrowType arrowType = ArrowType.Utf8.INSTANCE;
        when(mockField.getType()).thenReturn(arrowType);

        setupCaseInsensitiveConfig();

        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, mockField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(TEST_FIELD), extractorCaptor.capture());

        VarCharExtractor capturedExtractor = extractorCaptor.getValue();
        assertNotNull("VarCharExtractor should not be null", capturedExtractor);
    }

    @Test
    public void writeRowTemplate_WithCaseInsensitiveDisabled_CreatesVarCharExtractor()
    {
        ArrowType arrowType = ArrowType.Utf8.INSTANCE;
        when(mockField.getType()).thenReturn(arrowType);

        setupCaseSensitiveConfig();

        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, mockField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(TEST_FIELD), extractorCaptor.capture());

        VarCharExtractor capturedExtractor = extractorCaptor.getValue();
        assertNotNull("VarCharExtractor should not be null", capturedExtractor);
    }

    @Test
    public void writeRowTemplate_WithNullCaseInsensitiveConfig_CreatesVarCharExtractor()
    {
        ArrowType arrowType = ArrowType.Utf8.INSTANCE;
        when(mockField.getType()).thenReturn(arrowType);

        // configOptions is already initialized as empty HashMap in setUp()

        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, mockField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(TEST_FIELD), extractorCaptor.capture());

        VarCharExtractor capturedExtractor = extractorCaptor.getValue();
        assertNotNull("VarCharExtractor should not be null", capturedExtractor);
    }

    @Test
    public void writeRowTemplate_WithEmptyConfigOptions_CreatesVarCharExtractor()
    {
        ArrowType arrowType = ArrowType.Utf8.INSTANCE;
        when(mockField.getType()).thenReturn(arrowType);

        // configOptions is already initialized as empty HashMap in setUp()

        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, mockField, configOptions);

        ArgumentCaptor<VarCharExtractor> extractorCaptor = ArgumentCaptor.forClass(VarCharExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(TEST_FIELD), extractorCaptor.capture());

        VarCharExtractor capturedExtractor = extractorCaptor.getValue();
        assertNotNull("VarCharExtractor should not be null", capturedExtractor);
    }

    @Test(expected = NullPointerException.class)
    public void writeRowTemplate_WithNullConfigOptions_ThrowsNullPointerException()
    {
        ArrowType arrowType = ArrowType.Utf8.INSTANCE;
        when(mockField.getType()).thenReturn(arrowType);

        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, mockField, null);
    }

    @Test
    public void writeRowTemplate_WithBigIntField_CreatesBigIntExtractor()
    {
        ArrowType arrowType = new ArrowType.Int(64, true);
        when(mockField.getType()).thenReturn(arrowType);

        setupCaseInsensitiveConfig();

        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, mockField, configOptions);

        ArgumentCaptor<BigIntExtractor> extractorCaptor = ArgumentCaptor.forClass(BigIntExtractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(TEST_FIELD), extractorCaptor.capture());

        BigIntExtractor capturedExtractor = extractorCaptor.getValue();
        assertNotNull("BigIntExtractor should not be null", capturedExtractor);
    }

    @Test
    public void writeRowTemplate_WithFloat4Field_CreatesFloat4Extractor()
    {
        ArrowType arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        when(mockField.getType()).thenReturn(arrowType);

        setupCaseInsensitiveConfig();

        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, mockField, configOptions);

        ArgumentCaptor<Float4Extractor> extractorCaptor = ArgumentCaptor.forClass(Float4Extractor.class);
        verify(mockRowWriterBuilder).withExtractor(eq(TEST_FIELD), extractorCaptor.capture());

        Float4Extractor capturedExtractor = extractorCaptor.getValue();
        assertNotNull("Float4Extractor should not be null", capturedExtractor);
    }

    private void setupCaseSensitiveConfig()
    {
        configOptions.put(Constants.SCHEMA_CASE_INSEN, CASE_INSENSITIVE_FALSE);
    }

    private void setupCaseInsensitiveConfig()
    {
        configOptions.put(Constants.SCHEMA_CASE_INSEN, CASE_INSENSITIVE_TRUE);
    }
}
