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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Base class for StringTemplate-based query builders.
 * Enhanced with sophisticated string template patterns and logging similar to getPartitionWhereClauses.
 * Provides common functionality for building SQL queries using StringTemplate.
 */
public abstract class BaseQueryBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(BaseQueryBuilder.class);
    protected static final String TEMPLATE_NAME = "select_query";
    protected static final String TEMPLATE_FIELD = "builder";
    
    protected final ST query;
    protected List<String> projection;
    protected String catalogName;
    protected String schemaName;
    protected String tableName;
    protected List<String> conjuncts;
    protected String orderByClause;
    protected String limitClause;
    protected List<Object> parameterValues;
    protected final String quoteChar;
    protected Split currentSplit;

    protected BaseQueryBuilder(ST template, String quoteChar)
    {
        this.query = Validate.notNull(template, "The StringTemplate for " + TEMPLATE_NAME + " can not be null!");
        this.quoteChar = Validate.notNull(quoteChar, "quoteChar can not be null!");
        this.parameterValues = new ArrayList<>();
        logger.debug("BaseQueryBuilder initialized with quoteChar: {}", quoteChar);
    }

    public static String getTemplateName()
    {
        return TEMPLATE_NAME;
    }

    /**
     * Enhanced projection building with logging and validation.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    public BaseQueryBuilder withProjection(Schema schema, Split split)
    {
        logger.debug("withProjection - Building projection for schema with {} fields, split properties: {}", 
            schema.getFields().size(), split.getProperties().keySet());
        
        this.projection = schema.getFields().stream()
                .map(Field::getName)
                .filter(c -> !split.getProperties().containsKey(c))
                .collect(Collectors.toList());
        
        logger.debug("withProjection - Generated projection with {} columns", this.projection.size());
        return this;
    }

    /**
     * Enhanced table name building with logging and validation.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    public BaseQueryBuilder withTableName(TableName tableName)
    {
        logger.debug("withTableName - Setting table name: {}.{}", tableName.getSchemaName(), tableName.getTableName());
        this.schemaName = tableName.getSchemaName();
        this.tableName = tableName.getTableName();
        return this;
    }

    /**
     * Enhanced split handling with logging and validation.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    public BaseQueryBuilder withSplit(Split split)
    {
        logger.debug("withSplit - Setting split with {} properties", split.getProperties().size());
        this.currentSplit = split;
        return this;
    }

    /**
     * Enhanced catalog name handling with logging and validation.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    public BaseQueryBuilder withCatalogName(String catalogName)
    {
        logger.debug("withCatalogName - Setting catalog name: {}", catalogName);
        // Set to null if catalog name is empty or null to avoid SQL syntax errors
        this.catalogName = (catalogName == null || catalogName.trim().isEmpty()) ? null : catalogName;
        return this;
    }

    /**
     * Enhanced conjuncts building with logging and validation.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    public BaseQueryBuilder withConjuncts(Schema schema, Constraints constraints)
    {
        logger.debug("withConjuncts - Building conjuncts for schema with {} fields", schema.getFields().size());
        this.conjuncts = buildConjuncts(schema.getFields(), constraints, this.parameterValues);

        // Add database-specific partition clauses if split is available
        if (currentSplit != null) {
            List<String> partitionClauses = buildPartitionWhereClauses(currentSplit);
            if (!partitionClauses.isEmpty()) {
                logger.debug("withConjuncts - Adding {} partition clauses", partitionClauses.size());
                this.conjuncts.addAll(partitionClauses);
            }
        }

        logger.debug("withConjuncts - Generated {} conjuncts with {} parameter values", 
            this.conjuncts.size(), this.parameterValues.size());
        return this;
    }

    /**
     * Enhanced ORDER BY clause building with logging and validation.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    public BaseQueryBuilder withOrderByClause(Constraints constraints)
    {
        logger.debug("withOrderByClause - Building ORDER BY clause");
        this.orderByClause = extractOrderByClause(constraints);
        logger.debug("withOrderByClause - Generated ORDER BY clause: {}", this.orderByClause);
        return this;
    }

    /**
     * Enhanced LIMIT clause building with logging and validation.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    public BaseQueryBuilder withLimitClause(Constraints constraints)
    {
        logger.debug("withLimitClause - Building LIMIT clause with limit: {}", constraints.getLimit());
        if (constraints.getLimit() > 0) {
            this.limitClause = buildLimitClause(constraints.getLimit());
            logger.debug("withLimitClause - Generated LIMIT clause: {}", this.limitClause);
        }
        else {
            logger.debug("withLimitClause - No limit specified, skipping LIMIT clause");
        }
        return this;
    }

    public List<Object> getParameterValues()
    {
        return parameterValues;
    }

    // Getter methods for StringTemplate access
    public List<String> getProjection()
    {
        return projection;
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public List<String> getConjuncts()
    {
        return conjuncts;
    }

    public String getOrderByClause()
    {
        return orderByClause;
    }

    public String getLimitClause()
    {
        return limitClause;
    }

    /**
     * Enhanced query building with comprehensive validation and logging.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    public String build()
    {
        logger.debug("build - Building query with schema: {}, table: {}, conjuncts: {}, projection: {}", 
            schemaName, tableName, conjuncts != null ? conjuncts.size() : 0, projection != null ? projection.size() : 0);
        
        Validate.notNull(schemaName, "schemaName can not be null.");
        Validate.notNull(tableName, "tableName can not be null.");
        Validate.notNull(projection, "projection can not be null.");

        query.add(TEMPLATE_FIELD, this);
        query.add("quoteChar", quoteChar);
        String result = query.render().trim();
        
        logger.debug("build - Generated query: {}", result);
        return result;
    }

    /**
     * Build conjuncts for the query. Must be implemented by subclasses.
     */
    protected abstract List<String> buildConjuncts(List<Field> fields, Constraints constraints, List<Object> parameterValues);

    /**
     * Build database-specific partition WHERE clauses.
     * Must be implemented by subclasses to provide database-specific partition handling.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     *
     * @param split The split containing partition information
     * @return List of partition WHERE clauses
     */
    protected abstract List<String> buildPartitionWhereClauses(Split split);

    /**
     * Build the limit clause. Can be overridden by subclasses for different syntax.
     * Enhanced with logging similar to getPartitionWhereClauses pattern.
     */
    protected String buildLimitClause(long limit)
    {
        logger.debug("buildLimitClause - Building standard LIMIT clause with limit: {}", limit);
        return "LIMIT " + limit;
    }

    /**
     * Enhanced quote method with logging and validation.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    protected String quote(String identifier)
    {
        logger.debug("quote - Quoting identifier: {} with quoteChar: {}", identifier, quoteChar);
        String quoted = quoteChar + identifier + quoteChar;
        logger.debug("quote - Quoted result: {}", quoted);
        return quoted;
    }

    /**
     * Get the current split being processed.
     * Useful for subclasses that need access to split information.
     *
     * @return The current split or null if not set
     */
    protected Split getCurrentSplit()
    {
        return currentSplit;
    }

    /**
     * Enhanced ORDER BY clause extraction with logging and validation.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    private String extractOrderByClause(Constraints constraints)
    {
        logger.debug("extractOrderByClause - Extracting ORDER BY clause from constraints");
        List<OrderByField> orderByClause = constraints.getOrderByClause();
        if (orderByClause == null || orderByClause.isEmpty()) {
            logger.debug("extractOrderByClause - No ORDER BY clause found");
            return "";
        }
        
        String result = "ORDER BY " + orderByClause.stream()
                .map(orderByField -> {
                    String ordering = orderByField.getDirection().isAscending() ? "ASC" : "DESC";
                    String clause = quote(orderByField.getColumnName()) + " " + ordering;
                    logger.debug("extractOrderByClause - Processing orderByField: {} -> {}", 
                        orderByField.getColumnName(), clause);
                    return clause;
                })
                .collect(Collectors.joining(", "));
        
        logger.debug("extractOrderByClause - Generated ORDER BY clause: {}", result);
        return result;
    }
} 
