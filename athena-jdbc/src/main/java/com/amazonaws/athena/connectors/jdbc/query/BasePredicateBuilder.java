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
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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

    public List<String> buildConjuncts(List<Field> fields, Constraints constraints, List<TypeAndValue> accumulator)
    {
        ImmutableList.Builder<String> builder = ImmutableList.builder();

        for (Field column : fields) {
            ArrowType type = column.getType();
            if (constraints.getSummary() != null && !constraints.getSummary().isEmpty()) {
                ValueSet valueSet = constraints.getSummary().get(column.getName());
                if (valueSet != null) {
                    builder.add(toPredicate(column.getName(), valueSet, type, accumulator));
                }
            }
        }

        // Add complex expressions (OR conditions, etc.) from constraints.getExpression()
        builder.addAll(createFederationExpressionParser().parseComplexExpressions(fields, constraints, accumulator)); // not part of loop bc not per-column
        return builder.build();
    }

    protected String toPredicate(String columnName, ValueSet valueSet, ArrowType type, List<TypeAndValue> accumulator)
    {
        List<String> disjuncts = new ArrayList<>();
        List<Object> singleValues = new ArrayList<>();

//        // Handle EquatableValueSet (JDBC-specific, not in JdbcSplitQueryBuilder)
//        if (valueSet instanceof EquatableValueSet) {
//            EquatableValueSet equatableValueSet = (EquatableValueSet) valueSet;
//            com.amazonaws.athena.connector.lambda.data.Block valueBlock = equatableValueSet.getValues();
//
//            // Check if any values contain LIKE patterns
//            for (int i = 0; i < valueBlock.getRowCount(); i++) {
//                Object value = equatableValueSet.getValue(i);
//                if (value instanceof String && ((String) value).contains("%")) {
//                    accumulator.add(new TypeAndValue(type, convertValueForDatabase((String) value, type)));
//                    return renderTemplate("like_predicate", Map.of("columnName", columnName, "quoteChar", quoteChar));
//                }
//            }
//
//            if (valueBlock.getRowCount() == 0) {
//                return renderTemplate("null_predicate", Map.of("columnName", columnName, "isNull", false, "quoteChar", quoteChar));
//            }
//            else if (valueBlock.getRowCount() > 1) {
//                // Convert values to proper types
//                for (int i = 0; i < valueBlock.getRowCount(); i++) {
//                    Object value = equatableValueSet.getValue(i);
//                    Object convertedValue = convertValueForDatabase(value, type);
//                    accumulator.add(new TypeAndValue(type, convertedValue));
//                }
//
//                // Use IN clause template with correct counts parameter
//                List<String> placeholders = Collections.nCopies(valueBlock.getRowCount(), "?");
//                return renderTemplate("in_predicate", Map.of("columnName", columnName, "counts", placeholders, "quoteChar", quoteChar));
//            }
//            else if (valueBlock.getRowCount() == 1) {
//                accumulator.add(new TypeAndValue(type, convertValueForDatabase(equatableValueSet.getValue(0), type)));
//                return renderTemplate("comparison_predicate", Map.of("columnName", columnName, "operator", "=", "quoteChar", quoteChar));
//            }
//        }

        if (valueSet instanceof SortedRangeSet) {
            if (valueSet.isNone() && valueSet.isNullAllowed()) {
                return renderTemplate("null_predicate", Map.of("columnName", columnName, "isNull", true));
            }

            if (valueSet.isNullAllowed()) {
                disjuncts.add(renderTemplate("null_predicate", Map.of("columnName", columnName, "isNull", true)));
            }

            List<Range> rangeList = ((SortedRangeSet) valueSet).getOrderedRanges();
            if (rangeList.size() == 1 && !valueSet.isNullAllowed() && rangeList.get(0).getLow().isLowerUnbounded() && rangeList.get(0).getHigh().isUpperUnbounded()) {
                return renderTemplate("null_predicate", Map.of("columnName", columnName, "isNull", false));
            }

            for (Range range : valueSet.getRanges().getOrderedRanges()) {
                if (range.isSingleValue()) {
                    singleValues.add(range.getLow().getValue());
                }
                else {
                    List<String> rangeConjuncts = new ArrayList<>();
                    if (!range.getLow().isLowerUnbounded()) {
                        switch (range.getLow().getBound()) {
                            case ABOVE:
                                rangeConjuncts.add(toPredicate(columnName, ">", range.getLow().getValue(), type, accumulator));
                                break;
                            case EXACTLY:
                                rangeConjuncts.add(toPredicate(columnName, ">=", range.getLow().getValue(), type, accumulator));
                                break;
                            case BELOW:
                                throw new IllegalArgumentException("Low marker should never use BELOW bound");
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
                        }
                    }
                    if (!range.getHigh().isUpperUnbounded()) {
                        switch (range.getHigh().getBound()) {
                            case ABOVE:
                                throw new IllegalArgumentException("High marker should never use ABOVE bound");
                            case EXACTLY:
                                rangeConjuncts.add(toPredicate(columnName, "<=", range.getHigh().getValue(), type, accumulator));
                                break;
                            case BELOW:
                                rangeConjuncts.add(toPredicate(columnName, "<", range.getHigh().getValue(), type, accumulator));
                                break;
                            default:
                                throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
                        }
                    }
                    // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
                    Preconditions.checkState(!rangeConjuncts.isEmpty());
                    disjuncts.add(renderTemplate("range_predicate", Map.of("conjuncts", rangeConjuncts)));
                }
            }

            // Add back all of the possible single values either as an equality or an IN predicate
            if (singleValues.size() == 1) {
                disjuncts.add(toPredicate(columnName, "=", Iterables.getOnlyElement(singleValues), type, accumulator));
            }
            else if (singleValues.size() > 1) {
                for (Object value : singleValues) {
                    accumulator.add(new TypeAndValue(type, convertValueForDatabase(value, type)));
                }
                List<String> placeholders = Collections.nCopies(singleValues.size(), "?");
                disjuncts.add(renderTemplate("in_predicate", Map.of("columnName", columnName, "counts", placeholders)));
            }
        }

        return renderTemplate("or_predicate", Map.of("disjuncts", disjuncts));
    }

    protected String toPredicate(String columnName, String operator, Object value, ArrowType type, List<TypeAndValue> accumulator)
    {
        accumulator.add(new TypeAndValue(type, convertValueForDatabase(value, type)));
        return renderTemplate("comparison_predicate", Map.of("columnName", columnName, "operator", operator));
    }

    protected String renderTemplate(String templateName, Map<String, Object> params)
    {
        ST template = templateGroup.getInstanceOf(templateName);
        if (template == null) {
            throw new RuntimeException("Template not found: " + templateName);
        }

        // Add all parameters from the map
        params.forEach(template::add);

        return template.render().trim();
    }

    protected abstract Object convertValueForDatabase(Object value, org.apache.arrow.vector.types.pojo.ArrowType fieldType);

    /**
     * Create a FederationExpressionParser for handling complex expressions.
     * This method should be implemented by subclasses to provide database-specific parsers.
     *
     * @return A FederationExpressionParser instance
     */
    protected abstract FederationExpressionParser createFederationExpressionParser();
} 
