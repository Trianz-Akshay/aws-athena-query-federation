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
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import com.amazonaws.athena.connectors.jdbc.query.BaseQueryBuilder;
import com.amazonaws.athena.connectors.mysql.MySqlFederationExpressionParser;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * MySQL-specific query builder using refactored common functionality.
 * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
 * MySQL uses PARTITION(partition_name) syntax in FROM clause instead of WHERE clause.
 * <p>
 */
public class MySqlQueryBuilder extends BaseQueryBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(MySqlQueryBuilder.class);
    private final MySqlPredicateBuilder predicateBuilder;
    private final MySqlFederationExpressionParser federationExpressionParser;

    public MySqlQueryBuilder(ST template, String quoteChar)
    {
        super(template, quoteChar);
        this.predicateBuilder = new MySqlPredicateBuilder(quoteChar);
        this.federationExpressionParser = new MySqlFederationExpressionParser(quoteChar);
    }

    /**
     * Build MySQL-specific partition WHERE clauses.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     * MySQL handles partitioning differently - returns empty list as MySQL uses named partitions in FROM clause.
     */
    @Override
    protected List<String> buildPartitionWhereClauses(Split split)
    {
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
        this.conjuncts = buildConjuncts(schema.getFields(), constraints, this.parameterValues);

        // Add MySQL-specific partition clauses if split is available
        if (getCurrentSplit() != null) {
            List<String> partitionClauses = buildPartitionWhereClauses(getCurrentSplit());
            if (!partitionClauses.isEmpty()) {
                this.conjuncts.addAll(partitionClauses);
            }
        }

        return this;
    }

    /**
     * Enhanced ORDER BY clause building with MySQL-specific null ordering support.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    @Override
    public MySqlQueryBuilder withOrderByClause(Constraints constraints)
    {
        this.orderByClause = extractMySqlOrderByClause(constraints);
        return this;
    }

    /**
     * Enhanced table name building with MySQL-specific partition support.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    @Override
    public MySqlQueryBuilder withTableName(com.amazonaws.athena.connector.lambda.domain.TableName tableName)
    {
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
        String baseQuery = super.build();
        return applyMySqlFromClausePartition(baseQuery);
    }

    /**
     * Build conjuncts using the refactored predicate builder.
     */
    @Override
    protected List<String> buildConjuncts(List<Field> fields, Constraints constraints, List<TypeAndValue> accumulator)
    {
        return predicateBuilder.buildConjuncts(fields, constraints, accumulator);
    }

    /**
     * Extract MySQL-specific ORDER BY clause with null ordering support.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    private String extractMySqlOrderByClause(Constraints constraints)
    {
        List<OrderByField> orderByClause = constraints.getOrderByClause();
        if (orderByClause == null || orderByClause.isEmpty()) {
            return "";
        }

        return  "ORDER BY " + orderByClause.stream()
                .flatMap(orderByField -> {
                    String ordering = orderByField.getDirection().isAscending() ? "ASC" : "DESC";
                    String columnSorting = String.format("%s %s", quote(orderByField.getColumnName()), ordering);

                    switch (orderByField.getDirection()) {
                        case ASC_NULLS_FIRST:
                            // In MySQL ASC implies NULLS FIRST
                            logger.debug("extractMySqlOrderByClause - ASC_NULLS_FIRST: {}", columnSorting);
                        case DESC_NULLS_LAST:
                            // In MySQL DESC implies NULLS LAST
                            return java.util.stream.Stream.of(columnSorting);
                        case ASC_NULLS_LAST:
                            String ascNullsLast = String.format("ISNULL(%s) ASC", quote(orderByField.getColumnName()));
                            return java.util.stream.Stream.of(ascNullsLast, columnSorting);
                        case DESC_NULLS_FIRST:
                            String descNullsFirst = String.format("ISNULL(%s) DESC", quote(orderByField.getColumnName()));
                            return java.util.stream.Stream.of(descNullsFirst, columnSorting);
                    }
                    throw new UnsupportedOperationException("Unsupported sort order: " + orderByField.getDirection());
                })
                .collect(Collectors.joining(", "));
    }

    /**
     * Apply MySQL-specific FROM clause partition syntax.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     * MySQL uses PARTITION(partition_name) syntax in FROM clause instead of WHERE clause.
     */
    private String applyMySqlFromClausePartition(String baseQuery)
    {
        if (getCurrentSplit() == null) {
            return baseQuery;
        }

        String partitionName = getCurrentSplit().getProperty("partition_name");

        if (partitionName == null || "*".equals(partitionName)) {
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

        return baseQuery.replace(expectedFromClause, fromClauseWithPartition);
    }
} 
