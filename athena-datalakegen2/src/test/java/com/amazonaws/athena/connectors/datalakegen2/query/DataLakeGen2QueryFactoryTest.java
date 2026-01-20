/*-
 * #%L
 * athena-datalakegen2
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
package com.amazonaws.athena.connectors.datalakegen2.query;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

public class DataLakeGen2QueryFactoryTest
{
    private DataLakeGen2QueryFactory factory;

    @Before
    public void setUp()
    {
        factory = new DataLakeGen2QueryFactory();
    }

    @Test
    public void createQueryBuilder_WhenCalled_ReturnsQueryBuilder()
    {
        DataLakeGen2QueryBuilder builder = factory.createQueryBuilder();
        
        assertNotNull("Query builder should not be null", builder);
    }

    @Test
    public void createQueryBuilder_MultipleCalls_ReturnsNewInstances()
    {
        DataLakeGen2QueryBuilder builder1 = factory.createQueryBuilder();
        DataLakeGen2QueryBuilder builder2 = factory.createQueryBuilder();
        
        assertNotNull("First builder should not be null", builder1);
        assertNotNull("Second builder should not be null", builder2);
        // They should be different instances
        assertNotSame("Builders should be different instances", builder1, builder2);
    }

    @Test
    public void getQueryTemplate_WithValidTemplateName_ReturnsTemplate()
    {
        String templateName = DataLakeGen2QueryBuilder.getTemplateName();
        org.stringtemplate.v4.ST template = factory.getQueryTemplate(templateName);
        
        assertNotNull("Template should not be null", template);
    }
}
