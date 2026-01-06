/*-
 * #%L
 * athena-oracle
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
package com.amazonaws.athena.connectors.oracle;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import com.amazonaws.athena.connectors.oracle.query.OracleQueryBuilder;
import com.amazonaws.athena.connectors.oracle.query.OracleQueryFactory;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Utilities that help with SQL operations using StringTemplate.
 */
public class OracleSqlUtils
{
    private static final OracleQueryFactory queryFactory = new OracleQueryFactory();
    
    private OracleSqlUtils()
    {
    }
    
    /**
     * Gets the query factory instance for this connector.
     * Used by classes that need to render templates directly via JdbcSqlUtils.
     *
     * @return The OracleQueryFactory instance
     */
    public static OracleQueryFactory getQueryFactory()
    {
        return queryFactory;
    }
    
    /**
     * Builds an SQL statement from the schema, table name, split and constraints that can be executable by
     * Oracle using StringTemplate approach.
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
        OracleQueryBuilder queryBuilder = queryFactory.createQueryBuilder();
        
        String sql = queryBuilder
                .withCatalog(null)  // Oracle uses catalog.schema.table, but catalog is typically null in Athena context
                .withTableName(tableName)
                .withProjection(schema, split)
                .withPartitionClause(split)
                .withConjuncts(schema, constraints, split)
                .withOrderByClause(constraints)
                .withLimitClause(constraints)
                .build();

        // Copy the parameter values from the builder to the provided list
        parameterValues.clear();
        parameterValues.addAll(queryBuilder.getParameterValues());
        
        return sql;
    }
}

