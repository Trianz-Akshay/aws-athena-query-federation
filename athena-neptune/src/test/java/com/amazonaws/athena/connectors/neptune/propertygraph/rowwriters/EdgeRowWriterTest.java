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
import com.amazonaws.athena.connectors.neptune.Constants;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EdgeRowWriterTest
{
    @Mock
    private RowWriterBuilder mockRowWriterBuilder;
    
    @Mock
    private Field mockField;
    
    @Before
    public void setUp()
    {
        MockitoAnnotations.openMocks(this);
    }
    
    @Test
    public void testWriteRowTemplateWithBitField()
    {
        ArrowType arrowType = ArrowType.Bool.INSTANCE;
        when(mockField.getType()).thenReturn(arrowType);
        when(mockField.getName()).thenReturn("testField");
        
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(Constants.SCHEMA_CASE_INSEN, "true");
        
        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, mockField, configOptions);
        
        verify(mockRowWriterBuilder).withExtractor(eq("testField"), any());
    }
    
    @Test
    public void testWriteRowTemplateWithVarcharField()
    {
        ArrowType arrowType = ArrowType.Utf8.INSTANCE;
        when(mockField.getType()).thenReturn(arrowType);
        when(mockField.getName()).thenReturn("testField");
        
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(Constants.SCHEMA_CASE_INSEN, "true");
        
        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, mockField, configOptions);
        
        verify(mockRowWriterBuilder).withExtractor(eq("testField"), any());
    }
    
    @Test
    public void testWriteRowTemplateWithCaseInsensitiveDisabled()
    {
        ArrowType arrowType = ArrowType.Utf8.INSTANCE;
        when(mockField.getType()).thenReturn(arrowType);
        when(mockField.getName()).thenReturn("testField");
        
        Map<String, String> configOptions = new HashMap<>();
        configOptions.put(Constants.SCHEMA_CASE_INSEN, "false");
        
        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, mockField, configOptions);
        
        verify(mockRowWriterBuilder).withExtractor(eq("testField"), any());
    }
    
    @Test
    public void testWriteRowTemplateWithNullCaseInsensitiveConfig()
    {
        ArrowType arrowType = ArrowType.Utf8.INSTANCE;
        when(mockField.getType()).thenReturn(arrowType);
        when(mockField.getName()).thenReturn("testField");
        
        Map<String, String> configOptions = new HashMap<>();
        
        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, mockField, configOptions);
        
        verify(mockRowWriterBuilder).withExtractor(eq("testField"), any());
    }
    
    @Test
    public void testWriteRowTemplateWithEmptyConfigOptions()
    {
        ArrowType arrowType = ArrowType.Utf8.INSTANCE;
        when(mockField.getType()).thenReturn(arrowType);
        when(mockField.getName()).thenReturn("testField");
        
        Map<String, String> configOptions = new HashMap<>();
        
        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, mockField, configOptions);
        
        verify(mockRowWriterBuilder).withExtractor(eq("testField"), any());
    }
    
    @Test(expected = NullPointerException.class)
    public void testWriteRowTemplateWithNullConfigOptions()
    {
        ArrowType arrowType = ArrowType.Utf8.INSTANCE;
        when(mockField.getType()).thenReturn(arrowType);
        when(mockField.getName()).thenReturn("testField");
        
        EdgeRowWriter.writeRowTemplate(mockRowWriterBuilder, mockField, null);
    }
}
