/*-
 * #%L
 * Amazon Athena MySQL Connector
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
package com.amazonaws.athena.connectors.mysql.query;

import com.amazonaws.athena.connectors.jdbc.TestBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class MySqlQueryFactoryTest extends TestBase
{
    private MySqlQueryFactory queryFactory;
    private static final String MYSQL_QUOTE_CHAR = "`";

    @Before
    public void setup()
    {
        queryFactory = new MySqlQueryFactory(MYSQL_QUOTE_CHAR);
    }

    @Test
    public void testConstructor()
    {
        assertNotNull(queryFactory);
    }

    @Test
    public void testGetQuoteChar()
    {
        // Test that the factory was created successfully
        assertNotNull(queryFactory);
    }

    @Test
    public void testGetDatabaseQuoteChar()
    {
        String result = queryFactory.getDatabaseQuoteChar();
        assertEquals(MYSQL_QUOTE_CHAR, result);
    }

    @Test
    public void testCreateQueryBuilder()
    {
        MySqlQueryBuilder result = queryFactory.createQueryBuilder();

        assertNotNull(result);
        assertTrue(result instanceof MySqlQueryBuilder);
    }

    @Test
    public void testCreateQueryBuilderWithDifferentQuoteChar()
    {
        MySqlQueryFactory factoryWithDifferentQuote = new MySqlQueryFactory("\"");
        MySqlQueryBuilder result = factoryWithDifferentQuote.createQueryBuilder();

        assertNotNull(result);
        assertTrue(result instanceof MySqlQueryBuilder);
    }
} 
