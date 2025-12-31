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
import com.amazonaws.athena.connectors.jdbc.manager.JdbcPredicateBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcQueryBuilder;
import org.stringtemplate.v4.ST;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.amazonaws.athena.connectors.mysql.MySqlConstants.ALL_PARTITIONS;
import static com.amazonaws.athena.connectors.mysql.MySqlConstants.BLOCK_PARTITION_COLUMN_NAME;
import static com.amazonaws.athena.connectors.mysql.MySqlConstants.MYSQL_QUOTE_CHARACTER;

public class MySqlQueryBuilder extends JdbcQueryBuilder<MySqlQueryBuilder>
{
    private String partitionClause;

    public MySqlQueryBuilder(ST template)
    {
        super(template, MYSQL_QUOTE_CHARACTER);
    }

    @Override
    protected JdbcPredicateBuilder createPredicateBuilder()
    {
        return new MySqlPredicateBuilder();
    }

    @Override
    protected List<String> getPartitionWhereClauses(Split split)
    {
        // MySQL uses PARTITION clause in FROM, not WHERE
        return Collections.emptyList();
    }

    public MySqlQueryBuilder withPartitionClause(Split split)
    {
        String partitionName = split.getProperty(BLOCK_PARTITION_COLUMN_NAME);

        if (ALL_PARTITIONS.equals(partitionName)) {
            // No partitions
            this.partitionClause = "";
        }
        else if (partitionName != null) {
            this.partitionClause = "PARTITION(" + partitionName + ")";
        }
        else {
            this.partitionClause = "";
        }
        return this;
    }

    @Override
    public MySqlQueryBuilder withOrderByClause(Constraints constraints)
    {
        this.orderByClause = extractOrderByClause(constraints);
        return this;
    }

    public String getPartitionClause()
    {
        return partitionClause;
    }

    @Override
    protected String extractOrderByClause(Constraints constraints)
    {
        List<OrderByField> orderByFields = constraints.getOrderByClause();
        if (orderByFields == null || orderByFields.isEmpty()) {
            return "";
        }
        return "ORDER BY " + orderByFields.stream()
                .flatMap(orderByField -> {
                    String ordering = orderByField.getDirection().isAscending() ? "ASC" : "DESC";
                    String columnSorting = String.format("%s %s", quote(orderByField.getColumnName()), ordering);
                    switch (orderByField.getDirection()) {
                        case ASC_NULLS_FIRST:
                            // In MySQL ASC implies NULLS FIRST
                        case DESC_NULLS_LAST:
                            // In MySQL DESC implies NULLS LAST
                            return Stream.of(columnSorting);
                        case ASC_NULLS_LAST:
                            return Stream.of(String.format("ISNULL(%s) ASC", quote(orderByField.getColumnName())),
                                    columnSorting);
                        case DESC_NULLS_FIRST:
                            return Stream.of(
                                    String.format("ISNULL(%s) DESC", quote(orderByField.getColumnName())),
                                    columnSorting);
                    }
                    throw new UnsupportedOperationException("Unsupported sort order: " + orderByField.getDirection());
                })
                .collect(Collectors.joining(", "));
    }
}
