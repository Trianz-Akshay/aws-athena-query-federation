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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.OrderByField;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connectors.jdbc.query.EnhancedBaseQueryBuilder;
import com.amazonaws.athena.connectors.mysql.MySqlFederationExpressionParser;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * MySQL-specific query builder using refactored common functionality.
 * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
 * MySQL uses PARTITION(partition_name) syntax in FROM clause instead of WHERE clause.
 * <p>
 * This refactored version demonstrates how to use the new common classes to reduce code duplication.
 */
public class MySqlQueryBuilder extends EnhancedBaseQueryBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(MySqlQueryBuilder.class);
    private final MySqlPredicateBuilder predicateBuilder;
    private final MySqlFederationExpressionParser federationExpressionParser;

    public MySqlQueryBuilder(ST template, String quoteChar)
    {
        super(template, quoteChar);
        this.predicateBuilder = new MySqlPredicateBuilder(quoteChar);
        this.federationExpressionParser = new MySqlFederationExpressionParser(quoteChar);
        logger.debug("MySqlQueryBuilder - Initialized with quoteChar: {}", quoteChar);
    }

    /**
     * Build MySQL-specific partition WHERE clauses.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     * MySQL handles partitioning differently - returns empty list as MySQL uses named partitions in FROM clause.
     */
    @Override
    protected List<String> buildPartitionWhereClauses(Split split)
    {
        logger.debug("buildPartitionWhereClauses - Building MySQL partition clauses for split with {} properties",
                split.getProperties().size());

        // MySQL uses PARTITION(partition_name) syntax in FROM clause, not WHERE clause
        // So we return empty list for WHERE clauses
        return Collections.emptyList();
    }

    /**
     * Enhanced conjuncts building with refactored MySQL-specific predicate builder.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    @Override
    public MySqlQueryBuilder withConjuncts(org.apache.arrow.vector.types.pojo.Schema schema, Constraints constraints)
    {
        logger.debug("withConjuncts - Building conjuncts for schema with {} fields", schema.getFields().size());
        this.conjuncts = buildConjuncts(schema.getFields(), constraints, this.parameterValues);

        // Add MySQL-specific partition clauses if split is available
        if (getCurrentSplit() != null) {
            List<String> partitionClauses = buildPartitionWhereClauses(getCurrentSplit());
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
     * Enhanced ORDER BY clause building with MySQL-specific null ordering support.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    @Override
    public MySqlQueryBuilder withOrderByClause(Constraints constraints)
    {
        logger.debug("withOrderByClause - Building MySQL-specific ORDER BY clause");
        this.orderByClause = extractMySqlOrderByClause(constraints);
        logger.debug("withOrderByClause - Generated ORDER BY clause: {}", this.orderByClause);
        return this;
    }

    /**
     * Enhanced table name building with MySQL-specific partition support.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    @Override
    public MySqlQueryBuilder withTableName(com.amazonaws.athena.connector.lambda.domain.TableName tableName)
    {
        logger.debug("withTableName - Setting table name: {}.{}", tableName.getSchemaName(), tableName.getTableName());
        this.schemaName = tableName.getSchemaName();
        this.tableName = tableName.getTableName();
        return this;
    }

    /**
     * Enhanced catalog name handling with MySQL-specific logic.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    @Override
    public MySqlQueryBuilder withCatalogName(String catalogName)
    {
        logger.debug("withCatalogName - Setting catalog name: {}", catalogName);
        // Set to null if catalog name is empty or null to avoid SQL syntax errors
        this.catalogName = (catalogName == null || catalogName.trim().isEmpty()) ? null : catalogName;
        return this;
    }

    /**
     * Enhanced query building with MySQL-specific FROM clause partition support.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    @Override
    public String build()
    {
        logger.debug("build - Building MySQL query with schema: {}, table: {}, conjuncts: {}, projection: {}",
                schemaName, tableName, conjuncts != null ? conjuncts.size() : 0, projection != null ? projection.size() : 0);

        String baseQuery = super.build();
        String result = applyMySqlFromClausePartition(baseQuery);

        logger.debug("build - Generated MySQL query: {}", result);
        return result;
    }

    /**
     * Build conjuncts using the refactored predicate builder.
     */
    @Override
    protected List<String> buildConjuncts(List<Field> fields, Constraints constraints, List<Object> parameterValues)
    {
        logger.debug("buildConjuncts - Building MySQL conjuncts for {} fields", fields.size());

        // Debug the constraints
        if (constraints.getSummary() != null) {
            logger.debug("buildConjuncts - Constraints summary has {} entries", constraints.getSummary().size());
            for (Map.Entry<String, ValueSet> entry : constraints.getSummary().entrySet()) {
                logger.debug("buildConjuncts - Constraint for column '{}': {}", entry.getKey(), entry.getValue());
            }
        }
        else {
            logger.debug("buildConjuncts - No constraints summary");
        }

        List<String> conjuncts = predicateBuilder.buildConjuncts(fields, constraints, parameterValues);
        logger.debug("buildConjuncts - Generated {} MySQL conjuncts", conjuncts.size());

        // Debug the generated conjuncts
        for (int i = 0; i < conjuncts.size(); i++) {
            logger.debug("buildConjuncts - Conjunct {}: {}", i, conjuncts.get(i));
        }

        return conjuncts;
    }

    /**
     * Extract MySQL-specific ORDER BY clause with null ordering support.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    private String extractMySqlOrderByClause(Constraints constraints)
    {
        logger.debug("extractMySqlOrderByClause - Extracting MySQL ORDER BY clause from constraints");
        List<OrderByField> orderByClause = constraints.getOrderByClause();
        if (orderByClause == null || orderByClause.isEmpty()) {
            logger.debug("extractMySqlOrderByClause - No ORDER BY clause found");
            return "";
        }

        String result = "ORDER BY " + orderByClause.stream()
                .flatMap(orderByField -> {
                    String ordering = orderByField.getDirection().isAscending() ? "ASC" : "DESC";
                    String columnSorting = String.format("%s %s", quote(orderByField.getColumnName()), ordering);
                    logger.debug("extractMySqlOrderByClause - Processing orderByField: {} with direction: {}",
                            orderByField.getColumnName(), orderByField.getDirection());

                    switch (orderByField.getDirection()) {
                        case ASC_NULLS_FIRST:
                            // In MySQL ASC implies NULLS FIRST
                            logger.debug("extractMySqlOrderByClause - ASC_NULLS_FIRST: {}", columnSorting);
                        case DESC_NULLS_LAST:
                            // In MySQL DESC implies NULLS LAST
                            logger.debug("extractMySqlOrderByClause - DESC_NULLS_LAST: {}", columnSorting);
                            return java.util.stream.Stream.of(columnSorting);
                        case ASC_NULLS_LAST:
                            String ascNullsLast = String.format("ISNULL(%s) ASC", quote(orderByField.getColumnName()));
                            logger.debug("extractMySqlOrderByClause - ASC_NULLS_LAST: {} + {}", ascNullsLast, columnSorting);
                            return java.util.stream.Stream.of(ascNullsLast, columnSorting);
                        case DESC_NULLS_FIRST:
                            String descNullsFirst = String.format("ISNULL(%s) DESC", quote(orderByField.getColumnName()));
                            logger.debug("extractMySqlOrderByClause - DESC_NULLS_FIRST: {} + {}", descNullsFirst, columnSorting);
                            return java.util.stream.Stream.of(descNullsFirst, columnSorting);
                    }
                    throw new UnsupportedOperationException("Unsupported sort order: " + orderByField.getDirection());
                })
                .collect(Collectors.joining(", "));

        logger.debug("extractMySqlOrderByClause - Generated MySQL ORDER BY clause: {}", result);
        return result;
    }

    /**
     * Apply MySQL-specific FROM clause partition syntax.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     * MySQL uses PARTITION(partition_name) syntax in FROM clause instead of WHERE clause.
     */
    private String applyMySqlFromClausePartition(String baseQuery)
    {
        if (getCurrentSplit() == null) {
            logger.debug("applyMySqlFromClausePartition - No split available, returning base query");
            return baseQuery;
        }

        String partitionName = getCurrentSplit().getProperty("partition_name");
        logger.debug("applyMySqlFromClausePartition - partition_name: {}", partitionName);

        if (partitionName == null || "*".equals(partitionName)) {
            logger.debug("applyMySqlFromClausePartition - No specific partition or ALL_PARTITIONS, returning base query");
            return baseQuery;
        }

        // The StringTemplate generates: FROM `schema`.`table`
        // We need to replace it with: FROM `schema`.`table` PARTITION(partition_name)

        // Build the expected FROM clause pattern that StringTemplate generates
        // StringTemplate generates: FROM `schema`.`table` (no trailing space)
        String expectedFromClause;
        if (schemaName != null && !schemaName.trim().isEmpty()) {
            expectedFromClause = String.format(" FROM %s.%s",
                    quote(schemaName), quote(tableName));
        }
        else {
            expectedFromClause = String.format(" FROM %s", quote(tableName));
        }

        // Build the FROM clause with partition - add PARTITION clause after the table name
        String fromClauseWithPartition;
        if (schemaName != null && !schemaName.trim().isEmpty()) {
            fromClauseWithPartition = String.format(" FROM %s.%s PARTITION(%s)",
                    quote(schemaName), quote(tableName), partitionName);
        }
        else {
            fromClauseWithPartition = String.format(" FROM %s PARTITION(%s)",
                    quote(tableName), partitionName);
        }

        logger.info("applyMySqlFromClausePartition - Applying partition: {}", partitionName);
        logger.debug("applyMySqlFromClausePartition - Looking for FROM clause: {}", expectedFromClause);
        logger.debug("applyMySqlFromClausePartition - Replacing with: {}", fromClauseWithPartition);

        String modifiedQuery = baseQuery.replace(expectedFromClause, fromClauseWithPartition);

        logger.debug("applyMySqlFromClausePartition - Modified query: {}", modifiedQuery);
        return modifiedQuery;
    }
} 
