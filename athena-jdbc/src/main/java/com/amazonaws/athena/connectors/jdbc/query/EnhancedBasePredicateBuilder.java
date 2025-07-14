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

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.EquatableValueSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.Marker;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Enhanced base predicate builder that provides common functionality for database-specific predicate builders.
 * This class extracts common patterns found in MySQL and SQL Server predicate builders to reduce code duplication.
 * Enhanced with sophisticated string template patterns and logging similar to getPartitionWhereClauses.
 */
public abstract class EnhancedBasePredicateBuilder extends BasePredicateBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(EnhancedBasePredicateBuilder.class);

    protected EnhancedBasePredicateBuilder(STGroup templateGroup, String quoteChar)
    {
        super(templateGroup, quoteChar);
        logger.debug("EnhancedBasePredicateBuilder initialized with quoteChar: {}", quoteChar);
    }

    /**
     * Enhanced conjuncts building with type-aware processing.
     * Common implementation for database-specific predicate builders.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     *
     * @param fields The schema fields
     * @param constraints The query constraints
     * @param parameterValues List to collect parameter values
     * @return List of conjunct strings
     */
    @Override
    public List<String> buildConjuncts(List<Field> fields, Constraints constraints, List<Object> parameterValues)
    {
        List<String> conjuncts = new ArrayList<>();

        for (Field field : fields) {
            String columnName = field.getName();
            ValueSet valueSet = constraints.getSummary().get(columnName);

            if (valueSet != null) {
                logger.debug("buildConjuncts - Processing column: {}, valueSet: {}", columnName, valueSet);
                String conjunct = buildConjunct(columnName, valueSet, parameterValues, field.getType());
                if (conjunct != null) {
                    conjuncts.add(conjunct);
                    logger.debug("buildConjuncts - Added conjunct for column {}: {}", columnName, conjunct);
                }
                else {
                    logger.debug("buildConjuncts - No conjunct generated for column: {}", columnName);
                }
            }
        }

        // Add complex expressions (OR conditions, etc.) from constraints.getExpression()
        // This is crucial for handling OR conditions and other complex predicates
        if (constraints.getExpression() != null && !constraints.getExpression().isEmpty()) {
            logger.debug("buildConjuncts - Processing {} complex expressions", constraints.getExpression().size());
            
            // Create a FederationExpressionParser to handle complex expressions
            FederationExpressionParser federationExpressionParser = createFederationExpressionParser();
            
            // Create a TypeAndValue list that will collect parameters from complex expressions
            // This is similar to how the original JdbcSplitQueryBuilder works
            List<TypeAndValue> typeAndValueList = new ArrayList<>();
            
            // Add existing parameter values to the TypeAndValue list
            // Note: Existing parameters should already be converted by the individual buildConjunct methods
            // but we'll add them as-is to maintain the original parameter order
            for (Object value : parameterValues) {
                // We need to determine the type, but for now we'll use a default approach
                // This is a simplified version - in practice, you'd want to track types properly
                typeAndValueList.add(new TypeAndValue(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(), value));
            }
            
            List<String> complexExpressions = federationExpressionParser.parseComplexExpressions(fields, constraints, typeAndValueList);
            conjuncts.addAll(complexExpressions);
            
            // Extract new parameter values that were added by the complex expression parser
            // and add them to the original parameterValues list
            if (typeAndValueList.size() > parameterValues.size()) {
                for (int i = parameterValues.size(); i < typeAndValueList.size(); i++) {
                    TypeAndValue typeAndValue = typeAndValueList.get(i);
                    
                    // Convert the value using the database-specific converter to handle Arrow types
                    Object convertedValue = convertValueForDatabase(typeAndValue.getValue(), typeAndValue.getType());
                    parameterValues.add(convertedValue);
                    
                    logger.debug("buildConjuncts - Added complex expression parameter: {} (original type: {}, converted type: {})", 
                               typeAndValue.getValue(), 
                               typeAndValue.getValue() != null ? typeAndValue.getValue().getClass().getSimpleName() : "null",
                               convertedValue != null ? convertedValue.getClass().getSimpleName() : "null");
                }
            }
            
            logger.debug("buildConjuncts - Added {} complex expressions with {} new parameters", 
                        complexExpressions.size(), typeAndValueList.size() - parameterValues.size() + (typeAndValueList.size() - parameterValues.size()));
        }

        logger.debug("buildConjuncts - Final result: {} conjuncts", conjuncts.size());
        return conjuncts;
    }

    /**
     * Build a single conjunct for a column and its value set with type information.
     * Common implementation for database-specific predicate builders.
     *
     * @param columnName The column name
     * @param valueSet The value set for the column
     * @param parameterValues List to collect parameter values
     * @param fieldType The field type for proper value conversion
     * @return The conjunct string or null if no conjunct should be added
     */
    protected String buildConjunct(String columnName, ValueSet valueSet, List<Object> parameterValues, org.apache.arrow.vector.types.pojo.ArrowType fieldType)
    {
        logger.debug("buildConjunct - Column: {}, valueSet: {}, isNone: {}, isAll: {}, isNullAllowed: {}", 
                    columnName, valueSet.getClass().getSimpleName(), valueSet.isNone(), valueSet.isAll(), valueSet.isNullAllowed());
        
        if (valueSet.isNone()) {
            // Handle IS NULL case properly
            if (valueSet.isNullAllowed()) {
                logger.debug("buildConjunct - Generating IS NULL for column: {}", columnName);
                return buildNullPredicate(columnName, true); // IS NULL
            }
            else {
                logger.debug("buildConjunct - Generating IS NOT NULL for column: {}", columnName);
                return buildNullPredicate(columnName, false); // IS NOT NULL
            }
        }

        if (valueSet.isAll()) {
            // Special case: Check if this is actually an IS NOT NULL case
            if (valueSet instanceof SortedRangeSet) {
                SortedRangeSet rangeSet = (SortedRangeSet) valueSet;
                List<Range> ranges = rangeSet.getOrderedRanges();
                
                // Check for IS NOT NULL pattern: nullAllowed=false, single range with nullValue=true and ABOVE/BELOW bounds
                if (!rangeSet.isNullAllowed() && ranges.size() == 1) {
                    Range range = ranges.get(0);
                    if (range.getLow().isNullValue() && range.getHigh().isNullValue() &&
                        range.getLow().getBound() == Marker.Bound.ABOVE && 
                        range.getHigh().getBound() == Marker.Bound.BELOW) {
                        logger.debug("buildConjunct - Detected IS NOT NULL pattern for column: {} (isAll=true but special case)", columnName);
                        return buildNullPredicate(columnName, false); // IS NOT NULL
                    }
                }
            }
            
            logger.debug("buildConjunct - ValueSet is ALL, returning null for column: {}", columnName);
            return null;
        }

        if (valueSet instanceof SortedRangeSet) {
            logger.debug("buildConjunct - Processing SortedRangeSet for column: {}", columnName);
            return buildRangePredicate(columnName, (SortedRangeSet) valueSet, parameterValues, fieldType);
        }

        if (valueSet instanceof EquatableValueSet) {
            logger.debug("buildConjunct - Processing EquatableValueSet for column: {}", columnName);
            EquatableValueSet equatableValueSet = (EquatableValueSet) valueSet;
            com.amazonaws.athena.connector.lambda.data.Block valueBlock = equatableValueSet.getValues();

            // Check if any values contain LIKE patterns
            for (int i = 0; i < valueBlock.getRowCount(); i++) {
                Object value = equatableValueSet.getValue(i);
                if (value instanceof String && ((String) value).contains("%")) {
                    return buildLikePredicate(columnName, (String) value, parameterValues, fieldType);
                }
            }

            if (valueBlock.getRowCount() == 0) {
                return buildNullPredicate(columnName, false);
            }
            else if (valueBlock.getRowCount() > 1) {
                return buildInPredicate(columnName, equatableValueSet, parameterValues, fieldType);
            }
            else if (valueBlock.getRowCount() == 1) {
                return buildComparisonPredicate(columnName, "=", equatableValueSet.getValue(0), parameterValues, fieldType);
            }
        }

        logger.debug("buildConjunct - No conjunct generated for column: {}", columnName);
        return null;
    }

    /**
     * Build a range predicate for a column with type conversion.
     * Common implementation for database-specific predicate builders.
     *
     * @param columnName The column name
     * @param rangeSet The range set
     * @param parameterValues List to collect parameter values
     * @param fieldType The field type for proper value conversion
     * @return The range predicate string
     */
    protected String buildRangePredicate(String columnName, SortedRangeSet rangeSet, List<Object> parameterValues, org.apache.arrow.vector.types.pojo.ArrowType fieldType)
    {
        logger.debug("buildRangePredicate - Column: {}, Field type: {}, Ranges: {}", columnName, fieldType.getTypeID(), rangeSet.getOrderedRanges().size());

        // Handle special null cases like the original JdbcSplitQueryBuilder
        if (rangeSet.isNone() && rangeSet.isNullAllowed()) {
            logger.debug("buildRangePredicate - Handling IS NULL case for column: {}", columnName);
            return buildNullPredicate(columnName, true); // IS NULL
        }

        List<Range> ranges = rangeSet.getOrderedRanges();
        
        // Handle IS NOT NULL case like the original JdbcSplitQueryBuilder
        if (ranges.size() == 1 && !rangeSet.isNullAllowed() && 
            ranges.get(0).getLow().isLowerUnbounded() && ranges.get(0).getHigh().isUpperUnbounded()) {
            logger.debug("buildRangePredicate - Handling IS NOT NULL case for column: {}", columnName);
            return buildNullPredicate(columnName, false); // IS NOT NULL
        }
        
        // Handle special case where range has nullValue=true but represents IS NOT NULL
        if (ranges.size() == 1 && !rangeSet.isNullAllowed() && 
            ranges.get(0).getLow().isNullValue() && ranges.get(0).getHigh().isNullValue() &&
            ranges.get(0).getLow().getBound() == Marker.Bound.ABOVE && 
            ranges.get(0).getHigh().getBound() == Marker.Bound.BELOW) {
            logger.debug("buildRangePredicate - Handling IS NOT NULL case (nullValue=true) for column: {}", columnName);
            return buildNullPredicate(columnName, false); // IS NOT NULL
        }
        
        // Debug logging for the range
        if (ranges.size() == 1) {
            Range range = ranges.get(0);
            logger.debug("buildRangePredicate - Single range for column {}: low={}, high={}, isNullAllowed={}", 
                        columnName, range.getLow(), range.getHigh(), rangeSet.isNullAllowed());
        }

        // Check if all ranges are single values (for IN clause)
        boolean allSingleValues = true;
        for (Range range : ranges) {
            if (!range.isSingleValue()) {
                allSingleValues = false;
                break;
            }
        }

        if (allSingleValues && ranges.size() > 1) {
            // Convert multiple single values to IN clause
            logger.debug("buildRangePredicate - Converting {} single-value ranges to IN clause for column: {}", ranges.size(), columnName);

            // Create a mock EquatableValueSet for IN clause generation
            List<Object> values = new ArrayList<>();
            for (Range range : ranges) {
                Object value = range.getLow().getValue();
                if (value != null) {
                    Object convertedValue = convertValueForDatabase(value, fieldType);
                    values.add(convertedValue);
                    parameterValues.add(convertedValue);
                }
                else {
                    logger.debug("buildRangePredicate - Skipping null value in range for column: {}", columnName);
                }
            }

            // Use IN clause template
            ST template = templateGroup.getInstanceOf("in_predicate");
            template.add("columnName", columnName);
            template.add("counts", Collections.nCopies(values.size(), 1)); // Create list of 1s for each value
            template.add("quoteChar", quoteChar);

            String result = template.render();
            logger.debug("buildRangePredicate - Generated IN clause: {}", result);
            return result;
        }

        // Use OR logic for multiple ranges (similar to original JdbcSplitQueryBuilder)
        List<String> disjuncts = new ArrayList<>();

        // Add null handling like the original JdbcSplitQueryBuilder
        if (rangeSet.isNullAllowed()) {
            disjuncts.add(buildNullPredicate(columnName, true)); // IS NULL
            logger.debug("buildRangePredicate - Added IS NULL disjunct for column: {}", columnName);
        }

        for (Range range : ranges) {
            logger.debug("buildRangePredicate - Processing range: {}", range);

            if (range.isSingleValue()) {
                logger.debug("buildRangePredicate - Single value range, value: {} (type: {})",
                        range.getLow().getValue(), 
                        range.getLow().getValue() != null ? range.getLow().getValue().getClass().getSimpleName() : "null");

                Object convertedValue = convertValueForDatabase(range.getLow().getValue(), fieldType);
                parameterValues.add(convertedValue);

                ST template = templateGroup.getInstanceOf("comparison_predicate");
                template.add("columnName", columnName);
                template.add("operator", "=");
                template.add("quoteChar", quoteChar);

                String result = template.render();
                disjuncts.add(result);
                logger.debug("buildRangePredicate - Added equality predicate: {}", result);
            }
            else {
                // Handle range with low and high bounds - combine with AND within the range
                List<String> rangeConjuncts = new ArrayList<>();
                
                if (range.getLow().getBound() == Marker.Bound.EXACTLY) {
                    // Check if the marker has a null value
                    if (range.getLow().isNullValue()) {
                        logger.debug("buildRangePredicate - Skipping EXACTLY low bound with null value");
                    }
                    else {
                        Object convertedValue = convertValueForDatabase(range.getLow().getValue(), fieldType);
                        parameterValues.add(convertedValue);

                        ST template = templateGroup.getInstanceOf("comparison_predicate");
                        template.add("columnName", columnName);
                        template.add("operator", ">=");
                        template.add("quoteChar", quoteChar);

                        String result = template.render();
                        rangeConjuncts.add(result);
                        logger.debug("buildRangePredicate - Added >= predicate: {}", result);
                    }
                }
                else if (range.getLow().getBound() == Marker.Bound.ABOVE) {
                    // Check if the marker has a null value
                    if (range.getLow().isNullValue()) {
                        logger.debug("buildRangePredicate - Skipping ABOVE bound with null value");
                    }
                    else {
                        Object convertedValue = convertValueForDatabase(range.getLow().getValue(), fieldType);
                        parameterValues.add(convertedValue);

                        ST template = templateGroup.getInstanceOf("comparison_predicate");
                        template.add("columnName", columnName);
                        template.add("operator", ">");
                        template.add("quoteChar", quoteChar);

                        String result = template.render();
                        rangeConjuncts.add(result);
                        logger.debug("buildRangePredicate - Added > predicate: {}", result);
                    }
                }

                if (range.getHigh().getBound() == Marker.Bound.EXACTLY) {
                    // Check if the marker has a null value
                    if (range.getHigh().isNullValue()) {
                        logger.debug("buildRangePredicate - Skipping EXACTLY high bound with null value");
                    }
                    else {
                        Object convertedValue = convertValueForDatabase(range.getHigh().getValue(), fieldType);
                        parameterValues.add(convertedValue);

                        ST template = templateGroup.getInstanceOf("comparison_predicate");
                        template.add("columnName", columnName);
                        template.add("operator", "<=");
                        template.add("quoteChar", quoteChar);

                        String result = template.render();
                        rangeConjuncts.add(result);
                        logger.debug("buildRangePredicate - Added <= predicate: {}", result);
                    }
                }
                else if (range.getHigh().getBound() == Marker.Bound.BELOW) {
                    // Check if the marker has a null value
                    if (range.getHigh().isNullValue()) {
                        logger.debug("buildRangePredicate - Skipping BELOW bound with null value");
                    }
                    else {
                        Object convertedValue = convertValueForDatabase(range.getHigh().getValue(), fieldType);
                        parameterValues.add(convertedValue);

                        ST template = templateGroup.getInstanceOf("comparison_predicate");
                        template.add("columnName", columnName);
                        template.add("operator", "<");
                        template.add("quoteChar", quoteChar);

                        String result = template.render();
                        rangeConjuncts.add(result);
                        logger.debug("buildRangePredicate - Added < predicate: {}", result);
                    }
                }
                
                // Combine range conjuncts with AND (within the range)
                if (!rangeConjuncts.isEmpty()) {
                    ST rangeTemplate = templateGroup.getInstanceOf("range_predicate");
                    rangeTemplate.add("conjuncts", rangeConjuncts);
                    String rangeResult = rangeTemplate.render();
                    disjuncts.add(rangeResult);
                    logger.debug("buildRangePredicate - Added range predicate: {}", rangeResult);
                }
            }
        }

        // Combine all disjuncts with OR (between different ranges)
        if (disjuncts.size() == 1) {
            String result = disjuncts.get(0);
            logger.debug("buildRangePredicate - Single disjunct, returning: {}", result);
            return result;
        }
        else if (disjuncts.size() > 1) {
            // Use OR predicate template to combine multiple ranges with OR
            ST template = templateGroup.getInstanceOf("or_predicate");
            template.add("conjuncts", disjuncts);
            String result = template.render();
            logger.debug("buildRangePredicate - Generated OR predicate: {}", result);
            return result;
        }
        else {
            logger.debug("buildRangePredicate - No disjuncts generated");
            return null;
        }
    }

    /**
     * Build an IN predicate for multiple values with type conversion.
     * Common implementation for database-specific predicate builders.
     *
     * @param columnName The column name
     * @param equatableValueSet The equatable value set
     * @param parameterValues List to collect parameter values
     * @param fieldType The field type for proper value conversion
     * @return The IN predicate string
     */
    protected String buildInPredicate(String columnName, EquatableValueSet equatableValueSet, List<Object> parameterValues, org.apache.arrow.vector.types.pojo.ArrowType fieldType)
    {
        com.amazonaws.athena.connector.lambda.data.Block valueBlock = equatableValueSet.getValues();
        logger.debug("buildInPredicate - Column: {}, Field type: {}, Values: {}", columnName, fieldType.getTypeID(), valueBlock.getRowCount());

        // Convert values to proper types
        for (int i = 0; i < valueBlock.getRowCount(); i++) {
            Object value = equatableValueSet.getValue(i);
            Object convertedValue = convertValueForDatabase(value, fieldType);
            parameterValues.add(convertedValue);
            logger.debug("buildInPredicate - Added parameter: {} (original: {}, converted: {})", 
                convertedValue, value, convertedValue != null ? convertedValue.getClass().getSimpleName() : "null");
        }

        // Use IN clause template with correct counts parameter
        ST template = templateGroup.getInstanceOf("in_predicate");
        template.add("columnName", columnName);
        template.add("counts", Collections.nCopies(valueBlock.getRowCount(), 1));
        template.add("quoteChar", quoteChar);

        String result = template.render();
        logger.debug("buildInPredicate - Generated: {}", result);
        return result;
    }

    /**
     * Build a LIKE predicate with type conversion.
     * Common implementation for database-specific predicate builders.
     *
     * @param columnName The column name
     * @param pattern The LIKE pattern
     * @param parameterValues List to collect parameter values
     * @param fieldType The field type for proper value conversion
     * @return The LIKE predicate string
     */
    protected String buildLikePredicate(String columnName, String pattern, List<Object> parameterValues, org.apache.arrow.vector.types.pojo.ArrowType fieldType)
    {
        ST template = templateGroup.getInstanceOf("like_predicate");
        template.add("columnName", columnName);
        template.add("quoteChar", quoteChar);
        parameterValues.add(convertValueForDatabase(pattern, fieldType));
        return template.render();
    }

    /**
     * Build a comparison predicate with type conversion.
     * Common implementation for database-specific predicate builders.
     *
     * @param columnName The column name
     * @param operator The comparison operator (e.g., "=", ">", "<")
     * @param value The value to compare against
     * @param parameterValues List to collect parameter values
     * @param fieldType The field type for proper value conversion
     * @return The comparison predicate string
     */
    protected String buildComparisonPredicate(String columnName, String operator, Object value, List<Object> parameterValues, org.apache.arrow.vector.types.pojo.ArrowType fieldType)
    {
        ST template = templateGroup.getInstanceOf("comparison_predicate");
        template.add("columnName", columnName);
        template.add("operator", operator);
        template.add("quoteChar", quoteChar);
        parameterValues.add(convertValueForDatabase(value, fieldType));
        return template.render();
    }

    /**
     * Build a BETWEEN predicate with type conversion.
     * Common implementation for database-specific predicate builders.
     *
     * @param columnName The column name
     * @param lowerValue The lower bound value
     * @param upperValue The upper bound value
     * @param parameterValues List to collect parameter values
     * @param fieldType The field type for proper value conversion
     * @return The BETWEEN predicate string
     */
    protected String buildBetweenPredicate(String columnName, Object lowerValue, Object upperValue, List<Object> parameterValues, org.apache.arrow.vector.types.pojo.ArrowType fieldType)
    {
        ST template = templateGroup.getInstanceOf("between_predicate");
        template.add("columnName", columnName);
        template.add("quoteChar", quoteChar);
        parameterValues.add(convertValueForDatabase(lowerValue, fieldType));
        parameterValues.add(convertValueForDatabase(upperValue, fieldType));
        return template.render();
    }

    /**
     * Convert a value for the specific database type.
     * Must be implemented by subclasses to provide database-specific type conversion.
     *
     * @param value The value to convert
     * @param fieldType The field type for proper value conversion
     * @return The converted value
     */
    protected abstract Object convertValueForDatabase(Object value, org.apache.arrow.vector.types.pojo.ArrowType fieldType);

    /**
     * Convert a date value for the specific database type.
     * Common implementation for database-specific predicate builders.
     *
     * @param value The date value to convert
     * @param dateType The date type information
     * @return The converted date value
     */
    protected Object convertDateValueForDatabase(Object value, org.apache.arrow.vector.types.pojo.ArrowType.Date dateType)
    {
        if (value instanceof Date) {
            return value;
        }
        if (value instanceof Long) {
            long longValue = (Long) value;
            return new Date(longValue);
        }
        return value;
    }

    /**
     * Convert a timestamp value for the specific database type.
     * Common implementation for database-specific predicate builders.
     *
     * @param value The timestamp value to convert
     * @param timestampType The timestamp type information
     * @return The converted timestamp value
     */
    protected Object convertTimestampValueForDatabase(Object value, org.apache.arrow.vector.types.pojo.ArrowType.Timestamp timestampType)
    {
        if (value instanceof Timestamp) {
            return value;
        }
        if (value instanceof Long) {
            long longValue = (Long) value;
            return new Timestamp(longValue);
        }
        return value;
    }

    /**
     * Create a FederationExpressionParser for handling complex expressions.
     * This method should be implemented by subclasses to provide database-specific parsers.
     *
     * @return A FederationExpressionParser instance
     */
    protected abstract FederationExpressionParser createFederationExpressionParser();
} 
