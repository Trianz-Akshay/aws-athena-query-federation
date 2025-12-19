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
package com.amazonaws.athena.connectors.jdbc;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.query.BaseQueryBuilder;
import com.amazonaws.athena.connectors.jdbc.query.BaseQueryFactory;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Base utilities that help with SQL operations using StringTemplate.
 * Provides common functionality for building SQL statements.
 */
public class BaseSqlUtils
{
    private final BaseQueryFactory queryFactory;

    public BaseSqlUtils(BaseQueryFactory queryFactory)
    {
        this.queryFactory = queryFactory;
    }

    /**
     * Builds an SQL statement from the schema, table name, split and constraints that can be executable by
     * the database using StringTemplate approach.
     *
     * @param tableName       The table name of the table we are querying.
     * @param schema          The schema of the table that we are querying.
     * @param constraints     The constraints that we want to apply to the query.
     * @param parameterValues Query parameter values for parameterized query.
     * @param split
     * @return SQL Statement that represents the table, columns, split, and constraints.
     */
    public String buildSql(TableName tableName, Schema schema, Constraints constraints, List<Object> parameterValues, Split split)
    {
        BaseQueryBuilder queryBuilder = queryFactory.createQueryBuilder();

        String sql = queryBuilder
                .withTableName(tableName)
                .withSplit(split)  // Add split information for partition support
                .withProjection(schema, split)
                .withConjuncts(schema, constraints)
                .withOrderByClause(constraints)
                .withLimitClause(constraints)
                .build();

        // Copy the parameter values from the builder to the provided list
        parameterValues.clear();
        parameterValues.addAll(queryBuilder.getParameterValues());

        return sql;
    }
} 
