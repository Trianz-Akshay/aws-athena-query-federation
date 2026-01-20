/*-
 * #%L
 * athena-datalakegen2
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
package com.amazonaws.athena.connectors.datalakegen2;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.datalakegen2.query.DataLakeGen2QueryBuilder;
import com.amazonaws.athena.connectors.datalakegen2.query.DataLakeGen2QueryFactory;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Utilities that help with SQL operations using StringTemplate.
 */
public class DataLakeGen2SqlUtils
{
    private static final DataLakeGen2QueryFactory queryFactory = new DataLakeGen2QueryFactory();
    
    private DataLakeGen2SqlUtils()
    {
    }
    
    /**
     * Gets the query factory instance for this connector.
     * Used by classes that need to render templates directly via JdbcSqlUtils.
     *
     * @return The DataLakeGen2QueryFactory instance
     */
    public static DataLakeGen2QueryFactory getQueryFactory()
    {
        return queryFactory;
    }
    
    /**
     * Builds an SQL statement from the schema, table name, split and constraints that can be executable by
     * DataLakeGen2 using StringTemplate approach.
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
        DataLakeGen2QueryBuilder queryBuilder = queryFactory.createQueryBuilder();
        
        // DataLakeGen2 doesn't use catalog in FROM clause, only schema.table
        // Pass null for catalog to match old behavior
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
}
