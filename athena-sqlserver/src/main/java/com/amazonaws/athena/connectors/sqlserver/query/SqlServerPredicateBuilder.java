/*-
 * #%L
 * athena-sqlserver
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
package com.amazonaws.athena.connectors.sqlserver.query;

import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.query.BasePredicateBuilder;
import com.amazonaws.athena.connectors.sqlserver.SqlServerFederationExpressionParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.STGroupFile;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * SQL Server-specific predicate builder using refactored common functionality.
 * Converts date values to appropriate SQL Server types.
 * 
 * This refactored version demonstrates how to use the new common classes to reduce code duplication.
 */
public class SqlServerPredicateBuilder extends BasePredicateBuilder
{
    private static final String TEMPLATE_FILE = "JdbcQuery.stg";
    private static final Logger logger = LoggerFactory.getLogger(SqlServerPredicateBuilder.class);

    public SqlServerPredicateBuilder(String quoteChar)
    {
        super(new STGroupFile(TEMPLATE_FILE), quoteChar);
        logger.debug("SqlServerPredicateBuilder initialized with quoteChar: {}", quoteChar);
    }

    /**
     * Convert a value for SQL Server database type.
     * Implements the abstract method from BasePredicateBuilder.
     *
     * @param value The value to convert
     * @param fieldType The field type for proper value conversion
     * @return The converted value
     */
    @Override
    protected Object convertValueForDatabase(Object value, org.apache.arrow.vector.types.pojo.ArrowType fieldType)
    {
        logger.debug("convertValueForDatabase - Input value: {} (type: {}), Field type: {}",
                value, value != null ? value.getClass().getSimpleName() : "null", fieldType);
        
        if (value == null) {
            return null;
        }

        // Handle Arrow Text objects (convert to String)
        if (value instanceof org.apache.arrow.vector.util.Text) {
            String stringValue = value.toString();
            logger.debug("convertValueForDatabase - Converted Arrow Text to String: {}", stringValue);
            return stringValue;
        }

        // Handle date types
        if (fieldType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Date) {
            return convertDateValueForSqlServer(value, (org.apache.arrow.vector.types.pojo.ArrowType.Date) fieldType);
        }

        // Handle timestamp types
        if (fieldType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Timestamp) {
            return convertTimestampValueForSqlServer(value, (org.apache.arrow.vector.types.pojo.ArrowType.Timestamp) fieldType);
        }

        // Handle string types (Utf8)
        if (fieldType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Utf8) {
            if (value instanceof String) {
                return value;
            }
            else {
                String stringValue = value.toString();
                logger.debug("convertValueForDatabase - Converted to String: {}", stringValue);
                return stringValue;
            }
        }

        // Default case - return as-is
        logger.debug("convertValueForDatabase - Default case returning as-is: {} (type: {})",
                value, value.getClass().getSimpleName());
        return value;
    }

    /**
     * Create a SQL Server-specific FederationExpressionParser for handling complex expressions.
     * Implements the abstract method from BasePredicateBuilder.
     *
     * @return A FederationExpressionParser instance for SQL Server
     */
    @Override
    protected FederationExpressionParser createFederationExpressionParser()
    {
        return new SqlServerFederationExpressionParser(quoteChar);
    }

    /**
     * Convert a date value for SQL Server database type.
     * SQL Server-specific implementation for date type conversion.
     *
     * @param value The date value to convert
     * @param dateType The date type information
     * @return The converted date value
     */
    private Object convertDateValueForSqlServer(Object value, org.apache.arrow.vector.types.pojo.ArrowType.Date dateType)
    {
        logger.debug("convertDateValueForSqlServer - Converting date value: {} for SQL Server", value);
        
        if (value instanceof Date) {
            return value;
        }
        
        // Handle Date type - could be days since epoch (Number) or LocalDateTime
        if (value instanceof Number) {
            // Convert days since epoch to java.sql.Date
            long days = ((Number) value).longValue();
            long utcMillis = TimeUnit.DAYS.toMillis(days);

            // Adjust for timezone offset like the original implementation
            TimeZone defaultTimeZone = TimeZone.getDefault();
            int offset = defaultTimeZone.getOffset(utcMillis);
            utcMillis -= offset;

            Date result = new Date(utcMillis);
            logger.debug("convertDateValueForSqlServer - Date conversion: {} days -> {} ms -> {} (Date: {})",
                    days, utcMillis, result, result.toString());
            return result;
        }
        else if (value instanceof LocalDateTime) {
            // For Date(MILLISECOND) type, preserve time portion by using Timestamp
            // This is needed for SQL Server datetime columns that are mapped to Date(MILLISECOND)
            LocalDateTime localDateTime = (LocalDateTime) value;
            long utcMillis = localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();

            // Adjust for timezone offset like the original implementation
            TimeZone defaultTimeZone = TimeZone.getDefault();
            int offset = defaultTimeZone.getOffset(utcMillis);
            utcMillis -= offset;

            Timestamp result = new Timestamp(utcMillis);
            logger.debug("convertDateValueForSqlServer - Date(MILLISECOND) conversion from LocalDateTime: {} -> {} ms (offset: {}) -> {} (Timestamp: {})",
                    localDateTime, utcMillis + offset, offset, result, result.toString());
            return result;
        }
        else if (value instanceof Long) {
            long longValue = (Long) value;
            // SQL Server date conversion logic
            Date date = new Date(longValue);
            logger.debug("convertDateValueForSqlServer - Converted long {} to SQL Server date: {}", longValue, date);
            return date;
        }
        else {
            // For other types, try to convert to string and parse as date
            String result = String.valueOf(value);
            logger.debug("convertDateValueForSqlServer - Date conversion to string: {} -> {}", value, result);
            return result;
        }
    }

    /**
     * Convert a timestamp value for SQL Server database type.
     * SQL Server-specific implementation for timestamp type conversion.
     *
     * @param value The timestamp value to convert
     * @param timestampType The timestamp type information
     * @return The converted timestamp value
     */
    private Object convertTimestampValueForSqlServer(Object value, org.apache.arrow.vector.types.pojo.ArrowType.Timestamp timestampType)
    {
        logger.debug("convertTimestampValueForSqlServer - Converting timestamp value: {} for SQL Server", value);
        
        if (value instanceof Timestamp) {
            return value;
        }
        
        // Handle timestamp types - could be milliseconds since epoch (Number) or LocalDateTime
        if (value instanceof Number) {
            // Convert milliseconds since epoch to java.sql.Timestamp
            long millis = ((Number) value).longValue();
            
            // Adjust for timezone offset like the original implementation
            TimeZone defaultTimeZone = TimeZone.getDefault();
            int offset = defaultTimeZone.getOffset(millis);
            millis -= offset;

            Timestamp result = new Timestamp(millis);
            logger.debug("convertTimestampValueForSqlServer - Timestamp conversion: {} ms -> {} (offset: {}) -> {} (Timestamp: {})",
                    millis + offset, millis, offset, result, result.toString());
            return result;
        }
        else if (value instanceof LocalDateTime) {
            // Convert LocalDateTime to Timestamp
            LocalDateTime localDateTime = (LocalDateTime) value;
            long utcMillis = localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();

            // Adjust for timezone offset like the original implementation
            TimeZone defaultTimeZone = TimeZone.getDefault();
            int offset = defaultTimeZone.getOffset(utcMillis);
            utcMillis -= offset;

            Timestamp result = new Timestamp(utcMillis);
            logger.debug("convertTimestampValueForSqlServer - Timestamp conversion from LocalDateTime: {} -> {} ms (offset: {}) -> {} (Timestamp: {})",
                    localDateTime, utcMillis + offset, offset, result, result.toString());
            return result;
        }
        else if (value instanceof Long) {
            long longValue = (Long) value;
            // SQL Server timestamp conversion logic
            Timestamp timestamp = new Timestamp(longValue);
            logger.debug("convertTimestampValueForSqlServer - Converted long {} to SQL Server timestamp: {}", longValue, timestamp);
            return timestamp;
        }
        else {
            // For other types, try to convert to string
            String result = String.valueOf(value);
            logger.debug("convertTimestampValueForSqlServer - Timestamp conversion to string: {} -> {}", value, result);
            return result;
        }
    }
}
