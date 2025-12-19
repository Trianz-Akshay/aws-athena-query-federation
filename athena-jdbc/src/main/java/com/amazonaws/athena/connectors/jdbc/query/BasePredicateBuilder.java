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
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Base class for building SQL predicates using StringTemplate.
 * Provides common functionality for building WHERE clause conditions.
 * Enhanced with sophisticated string template patterns and logging.
 */
public abstract class BasePredicateBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(BasePredicateBuilder.class);

    protected final STGroup templateGroup;
    protected final String quoteChar;

    protected BasePredicateBuilder(STGroup templateGroup, String quoteChar)
    {
        this.templateGroup = Validate.notNull(templateGroup, "templateGroup can not be null!");
        this.quoteChar = Validate.notNull(quoteChar, "quoteChar can not be null!");
    }

    /**
     * Build conjuncts for the query based on the given fields and constraints.
     * Enhanced with type-aware processing and complex expression support.
     *
     * @param fields          The schema fields
     * @param constraints     The query constraints
     * @param parameterValues List to collect parameter values
     * @return List of conjunct strings
     */
    public List<String> buildConjuncts(List<Field> fields, Constraints constraints, List<Object> parameterValues)
    {
        List<String> conjuncts = new ArrayList<>();

        for (Field field : fields) {
            String columnName = field.getName();
            ValueSet valueSet = constraints.getSummary().get(columnName);

            if (valueSet != null) {
                String conjunct = buildConjunct(columnName, valueSet, parameterValues, field.getType());
                if (conjunct != null) {
                    conjuncts.add(conjunct);
                }
            }
        }

        // Add complex expressions (OR conditions, etc.) from constraints.getExpression()
        if (constraints.getExpression() != null && !constraints.getExpression().isEmpty()) {

            // Create a FederationExpressionParser to handle complex expressions
            FederationExpressionParser federationExpressionParser = createFederationExpressionParser();

            // Create a TypeAndValue list that will collect parameters from complex expressions
            List<TypeAndValue> typeAndValueList = new ArrayList<>();

            // Add existing parameter values to the TypeAndValue list
            for (Object value : parameterValues) {
                typeAndValueList.add(new TypeAndValue(org.apache.arrow.vector.types.Types.MinorType.VARCHAR.getType(), value));
            }

            List<String> complexExpressions = federationExpressionParser.parseComplexExpressions(fields, constraints, typeAndValueList);
            conjuncts.addAll(complexExpressions);

            // Extract new parameter values that were added by the complex expression parser
            if (typeAndValueList.size() > parameterValues.size()) {
                for (int i = parameterValues.size(); i < typeAndValueList.size(); i++) {
                    TypeAndValue typeAndValue = typeAndValueList.get(i);

                    // Convert the value using the database-specific converter to handle Arrow types
                    Object convertedValue = convertValueForDatabase(typeAndValue.getValue(), typeAndValue.getType());
                    parameterValues.add(convertedValue);
                }
            }
        }

        return conjuncts;
    }

    /**
     * Build a single conjunct for a column and its value set with type information.
     *
     * @param columnName      The column name
     * @param valueSet        The value set for the column
     * @param parameterValues List to collect parameter values
     * @param fieldType       The field type for proper value conversion
     * @return The conjunct string or null if no conjunct should be added
     */
    protected String buildConjunct(String columnName, ValueSet valueSet, List<Object> parameterValues, org.apache.arrow.vector.types.pojo.ArrowType fieldType)
    {

        if (valueSet.isNone()) {
            // Handle IS NULL case properly
            if (valueSet.isNullAllowed()) {
                return buildNullPredicate(columnName, true); // IS NULL
            }
            else {
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
                        return buildNullPredicate(columnName, false); // IS NOT NULL
                    }
                }
            }

            return null;
        }

        if (valueSet instanceof SortedRangeSet) {
            return buildRangePredicate(columnName, (SortedRangeSet) valueSet, parameterValues, fieldType);
        }

        if (valueSet instanceof EquatableValueSet) {
            EquatableValueSet equatableValueSet = (EquatableValueSet) valueSet;
            com.amazonaws.athena.connector.lambda.data.Block valueBlock = equatableValueSet.getValues();

            // Check if any values contain LIKE patterns
            for (int i = 0; i < valueBlock.getRowCount(); i++) {
                Object value = equatableValueSet.getValue(i);
                if (value instanceof String && ((String) value).contains("%")) {
                    ST template = templateGroup.getInstanceOf("like_predicate");
                    template.add("columnName", columnName);
                    template.add("quoteChar", quoteChar);
                    parameterValues.add(convertValueForDatabase((String) value, fieldType));
                    return template.render();
                }
            }

            if (valueBlock.getRowCount() == 0) {
                return buildNullPredicate(columnName, false);
            }
            else if (valueBlock.getRowCount() > 1) {
                // Convert values to proper types
                for (int i = 0; i < valueBlock.getRowCount(); i++) {
                    Object value = equatableValueSet.getValue(i);
                    Object convertedValue = convertValueForDatabase(value, fieldType);
                    parameterValues.add(convertedValue);
                }

                // Use IN clause template with correct counts parameter
                ST template = templateGroup.getInstanceOf("in_predicate");
                template.add("columnName", columnName);
                template.add("counts", Collections.nCopies(valueBlock.getRowCount(), 1));
                template.add("quoteChar", quoteChar);
                return template.render();
            }
            else if (valueBlock.getRowCount() == 1) {
                ST template = templateGroup.getInstanceOf("comparison_predicate");
                template.add("columnName", columnName);
                template.add("operator", "=");
                template.add("quoteChar", quoteChar);
                parameterValues.add(convertValueForDatabase(equatableValueSet.getValue(0), fieldType));
                return template.render();
            }
        }

        return null;
    }

    /**
     * Build a range predicate for a column with type conversion.
     *
     * @param columnName      The column name
     * @param rangeSet        The range set
     * @param parameterValues List to collect parameter values
     * @param fieldType       The field type for proper value conversion
     * @return The range predicate string
     */
    protected String buildRangePredicate(String columnName, SortedRangeSet rangeSet, List<Object> parameterValues, org.apache.arrow.vector.types.pojo.ArrowType fieldType)
    {

        // Handle special null cases
        if (rangeSet.isNone() && rangeSet.isNullAllowed()) {
            return buildNullPredicate(columnName, true); // IS NULL
        }

        List<Range> ranges = rangeSet.getOrderedRanges();

        // Handle IS NOT NULL case
        if (ranges.size() == 1 && !rangeSet.isNullAllowed() &&
                ranges.get(0).getLow().isLowerUnbounded() && ranges.get(0).getHigh().isUpperUnbounded()) {
            return buildNullPredicate(columnName, false); // IS NOT NULL
        }

        // Handle special case where range has nullValue=true but represents IS NOT NULL
        if (ranges.size() == 1 && !rangeSet.isNullAllowed() &&
                ranges.get(0).getLow().isNullValue() && ranges.get(0).getHigh().isNullValue() &&
                ranges.get(0).getLow().getBound() == Marker.Bound.ABOVE &&
                ranges.get(0).getHigh().getBound() == Marker.Bound.BELOW) {
            return buildNullPredicate(columnName, false); // IS NOT NULL
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
            // Create a mock EquatableValueSet for IN clause generation
            List<Object> values = new ArrayList<>();
            for (Range range : ranges) {
                Object value = range.getLow().getValue();
                if (value != null) {
                    Object convertedValue = convertValueForDatabase(value, fieldType);
                    values.add(convertedValue);
                    parameterValues.add(convertedValue);
                }
            }

            // Use IN clause template
            ST template = templateGroup.getInstanceOf("in_predicate");
            template.add("columnName", columnName);
            template.add("counts", Collections.nCopies(values.size(), 1)); // Create list of 1s for each value
            template.add("quoteChar", quoteChar);

            String result = template.render();
            return result;
        }

        // Use OR logic for multiple ranges
        List<String> disjuncts = new ArrayList<>();

        // Add null handling
        if (rangeSet.isNullAllowed()) {
            disjuncts.add(buildNullPredicate(columnName, true)); // IS NULL
        }

        for (Range range : ranges) {
            if (range.isSingleValue()) {

                Object convertedValue = convertValueForDatabase(range.getLow().getValue(), fieldType);
                parameterValues.add(convertedValue);

                ST template = templateGroup.getInstanceOf("comparison_predicate");
                template.add("columnName", columnName);
                template.add("operator", "=");
                template.add("quoteChar", quoteChar);

                String result = template.render();
                disjuncts.add(result);
            }
            else {
                // Handle range with low and high bounds - combine with AND within the range
                List<String> rangeConjuncts = new ArrayList<>();

                if (range.getLow().getBound() == Marker.Bound.EXACTLY) {
                    Object convertedValue = convertValueForDatabase(range.getLow().getValue(), fieldType);
                    parameterValues.add(convertedValue);

                    ST template = templateGroup.getInstanceOf("comparison_predicate");
                    template.add("columnName", columnName);
                    template.add("operator", ">=");
                    template.add("quoteChar", quoteChar);

                    String result = template.render();
                    rangeConjuncts.add(result);
                }
                else if (range.getLow().getBound() == Marker.Bound.ABOVE) {
                    Object convertedValue = convertValueForDatabase(range.getLow().getValue(), fieldType);
                    parameterValues.add(convertedValue);

                    ST template = templateGroup.getInstanceOf("comparison_predicate");
                    template.add("columnName", columnName);
                    template.add("operator", ">");
                    template.add("quoteChar", quoteChar);

                    String result = template.render();
                    rangeConjuncts.add(result);
                }

                if (range.getHigh().getBound() == Marker.Bound.EXACTLY) {
                    Object convertedValue = convertValueForDatabase(range.getHigh().getValue(), fieldType);
                    parameterValues.add(convertedValue);

                    ST template = templateGroup.getInstanceOf("comparison_predicate");
                    template.add("columnName", columnName);
                    template.add("operator", "<=");
                    template.add("quoteChar", quoteChar);

                    String result = template.render();
                    rangeConjuncts.add(result);
                }
                else if (range.getHigh().getBound() == Marker.Bound.BELOW) {
                    Object convertedValue = convertValueForDatabase(range.getHigh().getValue(), fieldType);
                    parameterValues.add(convertedValue);

                    ST template = templateGroup.getInstanceOf("comparison_predicate");
                    template.add("columnName", columnName);
                    template.add("operator", "<");
                    template.add("quoteChar", quoteChar);

                    String result = template.render();
                    rangeConjuncts.add(result);
                }

                // Combine range conjuncts with AND (within the range)
                if (!rangeConjuncts.isEmpty()) {
                    ST rangeTemplate = templateGroup.getInstanceOf("range_predicate");
                    rangeTemplate.add("conjuncts", rangeConjuncts);
                    String rangeResult = rangeTemplate.render();
                    disjuncts.add(rangeResult);
                }
            }
        }

        // Combine all disjuncts with OR (between different ranges)
        if (disjuncts.size() == 1) {
            String result = disjuncts.get(0);
            return result;
        }
        else if (disjuncts.size() > 1) {
            // Use OR predicate template to combine multiple ranges with OR
            ST template = templateGroup.getInstanceOf("or_predicate");
            template.add("conjuncts", disjuncts);
            String result = template.render();
            return result;
        }
        else {
            return null;
        }
    }


    /**
     * Build a NULL predicate using StringTemplate.
     *
     * @param columnName The column name
     * @param isNull     Whether checking for NULL (true) or NOT NULL (false)
     * @return The NULL predicate string
     */
    protected String buildNullPredicate(String columnName, boolean isNull)
    {
        ST template = templateGroup.getInstanceOf("null_predicate");
        template.add("columnName", columnName);
        template.add("isNull", isNull);
        template.add("quoteChar", quoteChar);
        return template.render();
    }

    /**
     * Convert a value for the specific database type.
     * Must be implemented by subclasses to provide database-specific type conversion.
     *
     * @param value     The value to convert
     * @param fieldType The field type for proper value conversion
     * @return The converted value
     */
    protected abstract Object convertValueForDatabase(Object value, org.apache.arrow.vector.types.pojo.ArrowType fieldType);

    /**
     * Create a FederationExpressionParser for handling complex expressions.
     * This method should be implemented by subclasses to provide database-specific parsers.
     *
     * @return A FederationExpressionParser instance
     */
    protected abstract FederationExpressionParser createFederationExpressionParser();
} 
