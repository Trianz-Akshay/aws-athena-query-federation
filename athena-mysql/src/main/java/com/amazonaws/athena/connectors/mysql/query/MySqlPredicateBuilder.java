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

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.query.EnhancedBasePredicateBuilder;
import com.amazonaws.athena.connectors.mysql.MySqlFederationExpressionParser;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.STGroupFile;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * MySQL-specific predicate builder using refactored common functionality.
 * Converts date values to appropriate MySQL types.
 * 
 * This refactored version demonstrates how to use the new common classes to reduce code duplication.
 */
public class MySqlPredicateBuilder extends EnhancedBasePredicateBuilder
{
    private static final String TEMPLATE_FILE = "JdbcQuery.stg";
    private static final Logger logger = LoggerFactory.getLogger(MySqlPredicateBuilder.class);

    public MySqlPredicateBuilder(String quoteChar)
    {
        super(new STGroupFile(TEMPLATE_FILE), quoteChar);
        logger.debug("MySqlPredicateBuilder initialized with quoteChar: {}", quoteChar);
    }

    /**
     * Override buildConjuncts to add MySQL-specific debug logging.
     */
    @Override
    public List<String> buildConjuncts(List<Field> fields, Constraints constraints, List<Object> parameterValues)
    {
        logger.debug("MySqlPredicateBuilder.buildConjuncts - Starting with {} fields", fields.size());
        
        // Debug the constraints before calling parent
        if (constraints.getSummary() != null) {
            for (Map.Entry<String, ValueSet> entry : constraints.getSummary().entrySet()) {
                String columnName = entry.getKey();
                ValueSet valueSet = entry.getValue();
                logger.debug("MySqlPredicateBuilder.buildConjuncts - Processing column '{}': valueSet={}, isNone={}, isAll={}, isNullAllowed={}", 
                           columnName, valueSet.getClass().getSimpleName(), valueSet.isNone(), valueSet.isAll(), valueSet.isNullAllowed());
                
                if (valueSet instanceof SortedRangeSet) {
                    SortedRangeSet rangeSet = (SortedRangeSet) valueSet;
                    List<Range> ranges = rangeSet.getOrderedRanges();
                    logger.debug("MySqlPredicateBuilder.buildConjuncts - SortedRangeSet for '{}': {} ranges", columnName, ranges.size());
                    
                    for (int i = 0; i < ranges.size(); i++) {
                        Range range = ranges.get(i);
                        logger.debug("MySqlPredicateBuilder.buildConjuncts - Range {} for '{}': low={}, high={}, isSingleValue={}", 
                                   i, columnName, range.getLow(), range.getHigh(), range.isSingleValue());
                        logger.debug("MySqlPredicateBuilder.buildConjuncts - Range {} low: isNullValue={}, bound={}", 
                                   i, range.getLow().isNullValue(), range.getLow().getBound());
                        logger.debug("MySqlPredicateBuilder.buildConjuncts - Range {} high: isNullValue={}, bound={}", 
                                   i, range.getHigh().isNullValue(), range.getHigh().getBound());
                    }
                }
            }
        }
        
        // Call parent method
        List<String> result = super.buildConjuncts(fields, constraints, parameterValues);
        
        logger.debug("MySqlPredicateBuilder.buildConjuncts - Parent returned {} conjuncts", result.size());
        for (int i = 0; i < result.size(); i++) {
            logger.debug("MySqlPredicateBuilder.buildConjuncts - Conjunct {}: {}", i, result.get(i));
        }
        
        return result;
    }

    /**
     * Convert a value for MySQL database type.
     * Implements the abstract method from EnhancedBasePredicateBuilder.
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
            return convertDateValueForMySql(value, (org.apache.arrow.vector.types.pojo.ArrowType.Date) fieldType);
        }

        // Handle timestamp types
        if (fieldType instanceof org.apache.arrow.vector.types.pojo.ArrowType.Timestamp) {
            return convertTimestampValueForMySql(value, (org.apache.arrow.vector.types.pojo.ArrowType.Timestamp) fieldType);
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
     * Create a MySQL-specific FederationExpressionParser for handling complex expressions.
     * Implements the abstract method from EnhancedBasePredicateBuilder.
     *
     * @return A FederationExpressionParser instance for MySQL
     */
    @Override
    protected FederationExpressionParser createFederationExpressionParser()
    {
        return new MySqlFederationExpressionParser(quoteChar);
    }

    /**
     * Convert a date value for MySQL database type.
     * MySQL-specific implementation for date type conversion.
     *
     * @param value The date value to convert
     * @param dateType The date type information
     * @return The converted date value
     */
    private Object convertDateValueForMySql(Object value, org.apache.arrow.vector.types.pojo.ArrowType.Date dateType)
    {
        logger.debug("convertDateValueForMySql - Converting date value: {} for MySQL", value);
        
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
            logger.debug("convertDateValueForMySql - Date conversion: {} days -> {} ms -> {} (Date: {})",
                    days, utcMillis, result, result.toString());
            return result;
        }
        else if (value instanceof LocalDateTime) {
            // For Date(MILLISECOND) type, preserve time portion by using Timestamp
            // This is needed for MySQL datetime columns that are mapped to Date(MILLISECOND)
            LocalDateTime localDateTime = (LocalDateTime) value;
            long utcMillis = localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();

            // Adjust for timezone offset like the original implementation
            TimeZone defaultTimeZone = TimeZone.getDefault();
            int offset = defaultTimeZone.getOffset(utcMillis);
            utcMillis -= offset;

            Timestamp result = new Timestamp(utcMillis);
            logger.debug("convertDateValueForMySql - Date(MILLISECOND) conversion from LocalDateTime: {} -> {} ms (offset: {}) -> {} (Timestamp: {})",
                    localDateTime, utcMillis + offset, offset, result, result.toString());
            return result;
        }
        else if (value instanceof Long) {
            long longValue = (Long) value;
            // MySQL date conversion logic
            Date date = new Date(longValue);
            logger.debug("convertDateValueForMySql - Converted long {} to MySQL date: {}", longValue, date);
            return date;
        }
        else {
            // For other types, try to convert to string and parse as date
            String result = String.valueOf(value);
            logger.debug("convertDateValueForMySql - Date conversion to string: {} -> {}", value, result);
            return result;
        }
    }

    /**
     * Convert a timestamp value for MySQL database type.
     * MySQL-specific implementation for timestamp type conversion.
     *
     * @param value The timestamp value to convert
     * @param timestampType The timestamp type information
     * @return The converted timestamp value
     */
    private Object convertTimestampValueForMySql(Object value, org.apache.arrow.vector.types.pojo.ArrowType.Timestamp timestampType)
    {
        logger.debug("convertTimestampValueForMySql - Converting timestamp value: {} for MySQL", value);
        
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
            logger.debug("convertTimestampValueForMySql - Timestamp conversion: {} ms -> {} (offset: {}) -> {} (Timestamp: {})",
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
            logger.debug("convertTimestampValueForMySql - Timestamp conversion from LocalDateTime: {} -> {} ms (offset: {}) -> {} (Timestamp: {})",
                    localDateTime, utcMillis + offset, offset, result, result.toString());
            return result;
        }
        else if (value instanceof Long) {
            long longValue = (Long) value;
            // MySQL timestamp conversion logic
            Timestamp timestamp = new Timestamp(longValue);
            logger.debug("convertTimestampValueForMySql - Converted long {} to MySQL timestamp: {}", longValue, timestamp);
            return timestamp;
        }
        else {
            // For other types, try to convert to string
            String result = String.valueOf(value);
            logger.debug("convertTimestampValueForMySql - Timestamp conversion to string: {} -> {}", value, result);
            return result;
        }
    }
} 
