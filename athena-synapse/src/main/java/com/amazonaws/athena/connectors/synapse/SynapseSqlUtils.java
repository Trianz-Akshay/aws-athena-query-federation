/*-
 * #%L
 * athena-synapse
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
package com.amazonaws.athena.connectors.synapse;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import com.amazonaws.athena.connectors.synapse.query.SynapseQueryBuilder;
import com.amazonaws.athena.connectors.synapse.query.SynapseQueryFactory;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Utilities that help with SQL operations using StringTemplate.
 */
public class SynapseSqlUtils
{
    private static final SynapseQueryFactory queryFactory = new SynapseQueryFactory();
    
    private SynapseSqlUtils()
    {
    }
    
    /**
     * Gets the query factory instance for this connector.
     * Used by classes that need to render templates directly via JdbcSqlUtils.
     *
     * @return The SynapseQueryFactory instance
     */
    public static SynapseQueryFactory getQueryFactory()
    {
        return queryFactory;
    }
    
    /**
     * Builds an SQL statement from the schema, table name, split and constraints that can be executable by
     * Azure Synapse using StringTemplate approach.
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
        SynapseQueryBuilder queryBuilder = queryFactory.createQueryBuilder();
        
        // Azure Synapse doesn't use catalog in FROM clause, only schema.table
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
