/*-
 * #%L
 * athena-snowflake
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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
package com.amazonaws.athena.connectors.snowflake;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSqlUtils;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import com.amazonaws.athena.connectors.snowflake.query.SnowflakeEmbeddedValuePredicateBuilder;
import com.amazonaws.athena.connectors.snowflake.query.SnowflakeQueryBuilder;
import com.amazonaws.athena.connectors.snowflake.query.SnowflakeQueryFactory;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_QUOTE_CHARACTER;

/**
 * Utilities that help with SQL operations using StringTemplate.
 */
public class SnowflakeSqlUtils
{
    private static final SnowflakeQueryFactory queryFactory = new SnowflakeQueryFactory();
    
    private SnowflakeSqlUtils()
    {
    }
    
    /**
     * Gets the query factory instance for this connector.
     * Used by classes that need to render templates directly via JdbcSqlUtils.
     *
     * @return The SnowflakeQueryFactory instance
     */
    public static SnowflakeQueryFactory getQueryFactory()
    {
        return queryFactory;
    }
    
    /**
     * Builds an SQL statement from the schema, table name, split and constraints that can be executable by
     * Snowflake using StringTemplate approach.
     *
     * @param tableName The table name of the table we are querying.
     * @param schema The schema of the table that we are querying.
     * @param constraints The constraints that we want to apply to the query.
     * @param split The split information (for partition support).
     * @param parameterValues List to store parameter values for the prepared statement.
     * @return SQL Statement that represents the table, columns, split, and constraints.
     */
    public static String buildSql(TableName tableName, Schema schema, Constraints constraints, Split split, List<TypeAndValue> parameterValues)
    {
        SnowflakeQueryBuilder queryBuilder = queryFactory.createQueryBuilder();
        
        // Snowflake uses schema.table format (no catalog in FROM clause)
        String sql = queryBuilder
                .withCatalog(null)
                .withTableName(tableName)
                .withProjection(schema, split)
                .withConjuncts(schema, constraints, split)
                .withOrderByClause(constraints)
                .withLimitClause(constraints)
                .build();

        // Copy the parameter values from the builder to the provided list
        parameterValues.clear();
        parameterValues.addAll(queryBuilder.getParameterValues());
        
        return sql;
    }
    
    /**
     * Quotes an identifier using Snowflake's quote character (double quotes).
     * 
     * @param identifier The identifier to quote
     * @return The quoted identifier
     */
    public static String quote(String identifier)
    {
        return JdbcSqlUtils.quoteIdentifier(identifier, SNOWFLAKE_QUOTE_CHARACTER);
    }
    
    /**
     * Escapes single quotes in a string literal by doubling them and wraps in single quotes.
     * 
     * @param value The string value to escape and quote
     * @return The escaped and quoted string
     */
    public static String singleQuote(String value)
    {
        if (value == null) {
            return null;
        }
        String escaped = value.replace("'", "''");
        return "'" + escaped + "'";
    }
    
    /**
     * Builds an SQL statement with embedded values (not parameterized) for use in S3 export scenarios.
     * This method embeds actual values directly in the SQL string instead of using parameterized queries.
     *
     * @param tableName The table name of the table we are querying.
     * @param schema The schema of the table that we are querying.
     * @param constraints The constraints that we want to apply to the query.
     * @param split The split information (for partition support).
     * @return SQL Statement with embedded values that represents the table, columns, split, and constraints.
     */
    public static String buildSqlWithEmbeddedValues(TableName tableName, Schema schema, Constraints constraints, Split split)
    {
        SnowflakeQueryBuilder queryBuilder = queryFactory.createQueryBuilder();
        
        // Use embedded value predicate builder instead of parameterized one
        queryBuilder.withPredicateBuilder(new SnowflakeEmbeddedValuePredicateBuilder());
        
        // Snowflake uses schema.table format (no catalog in FROM clause)
        String sql = queryBuilder
                .withCatalog(null)
                .withTableName(tableName)
                .withProjection(schema, split)
                .withConjuncts(schema, constraints, split)
                .withOrderByClause(constraints)
                .withLimitClause(constraints)
                .build();
        
        return sql;
    }
}
