/*-
 * #%L
 * Amazon Athena JDBC Connector
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
package com.amazonaws.athena.connectors.jdbc.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common query factory that provides shared functionality for database-specific query factories.
 * This class extracts common patterns found in MySQL and SQL Server query factories to reduce code duplication.
 * Enhanced with sophisticated string template patterns and logging similar to getPartitionWhereClauses.
 */
public abstract class CommonQueryFactory extends BaseQueryFactory
{
    private static final Logger logger = LoggerFactory.getLogger(CommonQueryFactory.class);
    private final String quoteChar;

    protected CommonQueryFactory(String templateFile, String quoteChar)
    {
        super(templateFile);
        this.quoteChar = quoteChar;
        logger.debug("CommonQueryFactory initialized with templateFile: {} and quoteChar: {}", templateFile, quoteChar);
    }

    /**
     * Enhanced quote character retrieval with logging.
     * Common implementation for database-specific query factories.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    @Override
    protected String getQuoteChar()
    {
        logger.debug("getQuoteChar - Returning quote character: {}", quoteChar);
        return quoteChar;
    }

    /**
     * Get the quote character for this database type.
     * Useful for subclasses that need access to the quote character.
     *
     * @return The quote character
     */
    protected String getDatabaseQuoteChar()
    {
        return quoteChar;
    }
} 
