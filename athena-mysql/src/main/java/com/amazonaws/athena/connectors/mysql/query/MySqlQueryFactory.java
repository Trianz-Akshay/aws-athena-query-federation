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

import com.amazonaws.athena.connectors.jdbc.query.BaseQueryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MySQL-specific query factory using refactored common functionality.
 * Enhanced with sophisticated string template patterns and logging similar to getPartitionWhereClauses.
 * Handles MySQL-specific ORDER BY clause with null ordering support.
 */
public class MySqlQueryFactory extends BaseQueryFactory
{
    private static final Logger logger = LoggerFactory.getLogger(MySqlQueryFactory.class);
    private static final String TEMPLATE_FILE = "JdbcQuery.stg";
    private static final String MYSQL_QUOTE_CHARACTER = "`";

    public MySqlQueryFactory(String quoteChar)
    {
        super(TEMPLATE_FILE, quoteChar);
    }

    @Override
    protected String getDatabaseQuoteChar()
    {
        return MYSQL_QUOTE_CHARACTER;
    }

    /**
     * Enhanced query builder creation with MySQL-specific logic and logging.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    @Override
    public MySqlQueryBuilder createQueryBuilder()
    {
        return new MySqlQueryBuilder(getQueryTemplate("select_query"), getQuoteChar());
    }
} 
