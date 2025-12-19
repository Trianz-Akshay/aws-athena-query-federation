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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.query.BaseQueryBuilder;
import com.amazonaws.athena.connectors.sqlserver.SqlServerFederationExpressionParser;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.util.Collections;
import java.util.List;

/**
 * SQL Server-specific query builder using refactored common functionality.
 * Enhanced with SQL Server-specific partition support similar to getPartitionWhereClauses.
 * SQL Server doesn't support LIMIT clause, so we return empty string.
 */
public class SqlServerQueryBuilder extends BaseQueryBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(SqlServerQueryBuilder.class);
    private final SqlServerPredicateBuilder predicateBuilder;
    private final SqlServerFederationExpressionParser federationExpressionParser;

    public SqlServerQueryBuilder(ST template, String quoteChar)
    {
        super(template, quoteChar);
        this.predicateBuilder = new SqlServerPredicateBuilder(quoteChar);
        this.federationExpressionParser = new SqlServerFederationExpressionParser(quoteChar);
    }

    /**
     * Build SQL Server-specific partition WHERE clauses.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     * Example query: select * from MyPartitionTable where $PARTITION.myRangePF(col1) = 2
     */
    @Override
    protected List<String> buildPartitionWhereClauses(Split split)
    {
        String partitionFunction = split.getProperty("PARTITION_FUNCTION");
        String partitioningColumn = split.getProperty("PARTITIONING_COLUMN");
        String partitionNumber = split.getProperty("partition_number");


        if (partitionFunction != null && partitioningColumn != null && partitionNumber != null && !partitionNumber.equals("0")) {

            String partitionClause = String.format(" $PARTITION.%s(%s) = %s",
                    partitionFunction, partitioningColumn, partitionNumber);

            return Collections.singletonList(partitionClause);
        }
        return Collections.emptyList();
    }

    /**
     * Enhanced table name building with SQL Server-specific partition support.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    @Override
    public SqlServerQueryBuilder withTableName(com.amazonaws.athena.connector.lambda.domain.TableName tableName)
    {
        this.schemaName = tableName.getSchemaName();
        this.tableName = tableName.getTableName();
        return this;
    }

    /**
     * Enhanced conjuncts building with refactored SQL Server-specific predicate builder.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    @Override
    public SqlServerQueryBuilder withConjuncts(org.apache.arrow.vector.types.pojo.Schema schema, Constraints constraints)
    {
        this.conjuncts = buildConjuncts(schema.getFields(), constraints, this.parameterValues);

        // Add SQL Server-specific partition clauses if split is available
        if (getCurrentSplit() != null) {
            List<String> partitionClauses = buildPartitionWhereClauses(getCurrentSplit());
            if (!partitionClauses.isEmpty()) {
                this.conjuncts.addAll(partitionClauses);
            }
        }

        return this;
    }

    /**
     * Enhanced LIMIT clause building with SQL Server-specific behavior.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     * SQL Server doesn't support LIMIT clause, so we return empty string.
     */
    @Override
    protected String buildLimitClause(long limit)
    {
        // SQL Server doesn't support LIMIT clause, so we return empty string
        return "";
    }

    /**
     * Build conjuncts using the refactored predicate builder.
     */
    @Override
    protected List<String> buildConjuncts(List<Field> fields, Constraints constraints, List<Object> parameterValues)
    {
        return predicateBuilder.buildConjuncts(fields, constraints, parameterValues);
    }

    /**
     * Enhanced catalog name handling with SQL Server-specific logic.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    @Override
    public SqlServerQueryBuilder withCatalogName(String catalogName)
    {
        // Set to null if catalog name is empty or null to avoid SQL syntax errors
        this.catalogName = (catalogName == null || catalogName.trim().isEmpty()) ? null : catalogName;
        return this;
    }

    /**
     * Enhanced query building with SQL Server-specific logic.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    @Override
    public String build()
    {
        return super.build();
    }
}
