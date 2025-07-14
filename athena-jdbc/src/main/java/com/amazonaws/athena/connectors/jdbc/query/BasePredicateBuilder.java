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
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.Validate;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroup;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for building SQL predicates using StringTemplate.
 * Provides common functionality for building WHERE clause conditions.
 */
public abstract class BasePredicateBuilder
{
    protected final STGroup templateGroup;
    protected final String quoteChar;

    protected BasePredicateBuilder(STGroup templateGroup, String quoteChar)
    {
        this.templateGroup = Validate.notNull(templateGroup, "templateGroup can not be null!");
        this.quoteChar = Validate.notNull(quoteChar, "quoteChar can not be null!");
    }

    /**
     * Build conjuncts for the query based on the given fields and constraints.
     *
     * @param fields The schema fields
     * @param constraints The query constraints
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
                String conjunct = buildConjunct(columnName, valueSet, parameterValues);
                if (conjunct != null) {
                    conjuncts.add(conjunct);
                }
            }
        }
        
        return conjuncts;
    }

    /**
     * Build a single conjunct for a column and its value set.
     *
     * @param columnName The column name
     * @param valueSet The value set for the column
     * @param parameterValues List to collect parameter values
     * @return The conjunct string or null if no conjunct should be added
     */
    protected String buildConjunct(String columnName, ValueSet valueSet, List<Object> parameterValues)
    {
        if (valueSet.isNone()) {
            return buildNullPredicate(columnName, false);
        }
        
        if (valueSet.isAll()) {
            return null;
        }
        
        if (valueSet instanceof SortedRangeSet) {
            return buildRangePredicate(columnName, (SortedRangeSet) valueSet, parameterValues);
        }
        
        return null;
    }

    /**
     * Build a range predicate for a column.
     *
     * @param columnName The column name
     * @param rangeSet The range set
     * @param parameterValues List to collect parameter values
     * @return The range predicate string
     */
    protected String buildRangePredicate(String columnName, SortedRangeSet rangeSet, List<Object> parameterValues)
    {
        List<String> conjuncts = new ArrayList<>();
        
        for (Range range : rangeSet.getOrderedRanges()) {
            if (range.isSingleValue()) {
                conjuncts.add(buildComparisonPredicate(columnName, "="));
                parameterValues.add(range.getLow().getValue());
            }
            else {
                // Handle range with low and high bounds
                if (range.getLow().getBound() == Marker.Bound.EXACTLY) {
                    conjuncts.add(buildComparisonPredicate(columnName, ">="));
                    parameterValues.add(range.getLow().getValue());
                }
                
                if (range.getHigh().getBound() == Marker.Bound.EXACTLY) {
                    conjuncts.add(buildComparisonPredicate(columnName, "<="));
                    parameterValues.add(range.getHigh().getValue());
                }
            }
        }
        
        // Use range_predicate template to combine with AND
        ST template = templateGroup.getInstanceOf("range_predicate");
        template.add("conjuncts", conjuncts);
        
        return template.render();
    }

    /**
     * Build an IN predicate for multiple values.
     *
     * @param columnName The column name
     * @param equatableValueSet The equatable value set
     * @param parameterValues List to collect parameter values
     * @return The IN predicate string
     */
    protected String buildInPredicate(String columnName, EquatableValueSet equatableValueSet, List<Object> parameterValues)
    {
        // Add all values to parameter list
        for (int i = 0; i < equatableValueSet.getValues().getRowCount(); i++) {
            parameterValues.add(equatableValueSet.getValue(i));
        }
        
        // Use IN clause template
        ST template = templateGroup.getInstanceOf("in_predicate");
        template.add("columnName", columnName);
        template.add("counts", equatableValueSet.getValues().getRowCount());
        template.add("quoteChar", quoteChar);
        
        return template.render();
    }

    /**
     * Build a NULL predicate using StringTemplate.
     *
     * @param columnName The column name
     * @param isNull Whether checking for NULL (true) or NOT NULL (false)
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
     * Build a range predicate using StringTemplate.
     *
     * @param conjuncts The list of conjuncts to join
     * @return The range predicate string
     */
    protected String buildRangePredicate(List<String> conjuncts)
    {
        ST template = templateGroup.getInstanceOf("range_predicate");
        template.add("conjuncts", conjuncts);
        return template.render();
    }

    /**
     * Build an OR predicate using StringTemplate.
     *
     * @param disjuncts The list of disjuncts to join
     * @param wrapInParens Whether to wrap each disjunct in parentheses
     * @return The OR predicate string
     */
    protected String buildOrPredicate(List<String> disjuncts, boolean wrapInParens)
    {
        ST template = templateGroup.getInstanceOf("or_predicate_with_wrapping");
        template.add("disjuncts", disjuncts);
        template.add("wrapInParens", wrapInParens);
        return template.render();
    }

    /**
     * Build a BETWEEN predicate using StringTemplate.
     *
     * @param columnName The column name
     * @return The BETWEEN predicate string
     */
    protected String buildBetweenPredicate(String columnName)
    {
        ST template = templateGroup.getInstanceOf("between_predicate");
        template.add("columnName", columnName);
        template.add("quoteChar", quoteChar);
        return template.render();
    }

    /**
     * Build a comparison predicate using StringTemplate.
     *
     * @param columnName The column name
     * @param operator The comparison operator (e.g., "=", ">", "<")
     * @return The comparison predicate string
     */
    protected String buildComparisonPredicate(String columnName, String operator)
    {
        ST template = templateGroup.getInstanceOf("comparison_predicate");
        template.add("columnName", columnName);
        template.add("operator", operator);
        template.add("quoteChar", quoteChar);
        return template.render();
    }
} 
