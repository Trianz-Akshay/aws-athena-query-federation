/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.jdbc.query;

import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupFile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BaseQueryFactoryTest
{
    private TestBaseQueryFactory queryFactory;

    @Before
    public void setup()
    {
        queryFactory = new TestBaseQueryFactory("test_template.stg", "test_quote");
    }

    @Test
    public void testGetQueryTemplate()
    {
        ST template = queryFactory.getQueryTemplate("test_template");
        assertNotNull(template);
    }

    @Test
    public void testCreateQueryBuilder()
    {
        BaseQueryBuilder queryBuilder = queryFactory.createQueryBuilder();
        assertNotNull(queryBuilder);
        // Test that the query builder can be created
        assertEquals("test_quotetesttest_quote", queryBuilder.quote("test"));
    }

    @Test
    public void testGetQuoteChar()
    {
        String quoteChar = queryFactory.getQuoteChar();
        assertEquals("test_quote", quoteChar);
    }

    // Test implementation of BaseQueryFactory for testing
    private static class TestBaseQueryFactory extends BaseQueryFactory
    {
        public TestBaseQueryFactory(String templateFile, String quoteChar)
        {
            super(templateFile, quoteChar);
        }

        @Override
        public BaseQueryBuilder createQueryBuilder()
        {
            ST mockTemplate = Mockito.mock(ST.class);
            Mockito.lenient().when(mockTemplate.add(any(), any())).thenReturn(mockTemplate);
            Mockito.lenient().when(mockTemplate.render()).thenReturn("SELECT * FROM test_table");
            
            return new TestBaseQueryBuilder(mockTemplate, getQuoteChar());
        }



        @Override
        protected STGroupFile createGroupFile()
        {
            STGroupFile mockGroupFile = Mockito.mock(STGroupFile.class);
            ST mockTemplate = Mockito.mock(ST.class);
            when(mockGroupFile.getInstanceOf("test_template")).thenReturn(mockTemplate);
            return mockGroupFile;
        }
    }

    // Test implementation of BaseQueryBuilder for testing
    private static class TestBaseQueryBuilder extends BaseQueryBuilder
    {
        public TestBaseQueryBuilder(ST template, String quoteChar)
        {
            super(template, quoteChar);
        }

        @Override
        protected java.util.List<String> buildConjuncts(java.util.List<Field> fields, 
                                                       com.amazonaws.athena.connector.lambda.domain.predicate.Constraints constraints, 
                                                       java.util.List<TypeAndValue> accumulator)
        {
            return java.util.Collections.emptyList();
        }

        @Override
        protected java.util.List<String> buildPartitionWhereClauses(com.amazonaws.athena.connector.lambda.domain.Split split)
        {
            return java.util.Collections.emptyList();
        }
    }
} 