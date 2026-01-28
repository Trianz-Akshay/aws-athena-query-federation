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
package com.amazonaws.athena.connectors.synapse.query;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcPredicateBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcQueryBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSqlUtils;
import com.amazonaws.athena.connectors.synapse.SynapseMetadataHandler;
import com.amazonaws.athena.connectors.synapse.SynapseSqlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.synapse.SynapseConstants.QUOTE_CHARACTER;

public class SynapseQueryBuilder extends JdbcQueryBuilder<SynapseQueryBuilder>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SynapseQueryBuilder.class);

    public SynapseQueryBuilder(ST template)
    {
        super(template, QUOTE_CHARACTER);
    }
    
    @Override
    protected JdbcPredicateBuilder createPredicateBuilder()
    {
        return new SynapsePredicateBuilder();
    }
    
    /**
     * Handles Azure Synapse range partitioning by building appropriate WHERE clauses
     * based on partition boundaries stored in split properties.
     */
    @Override
    protected List<String> getPartitionWhereClauses(Split split)
    {
        String column = split.getProperty(SynapseMetadataHandler.PARTITION_COLUMN);
        if (column != null) {
            LOGGER.debug("Fetching data using Partition");
            String from = split.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_FROM);
            String to = split.getProperty(SynapseMetadataHandler.PARTITION_BOUNDARY_TO);

            LOGGER.debug("PARTITION_COLUMN: {}", column);
            LOGGER.debug("PARTITION_BOUNDARY_FROM: {}", from);
            LOGGER.debug("PARTITION_BOUNDARY_TO: {}", to);

            /*
                Form where clause using partition boundaries to create specific partition as split
                Example query: select * from MyPartitionTable where id > 1 and id <= 100
             */
            if (from != null && !from.trim().isEmpty() && to != null && !to.trim().isEmpty()) {
                // Both boundaries exist: column > from and column <= to
                String partitionClause = JdbcSqlUtils.renderTemplate(
                        SynapseSqlUtils.getQueryFactory(),
                        "partition_range_clause",
                        Map.of("partitionColumn", column,
                                "partitionBoundaryFrom", from,
                                "partitionBoundaryTo", to));
                return Collections.singletonList(partitionClause);
            }
            else if ((from == null || from.trim().isEmpty()) && (to == null || to.trim().isEmpty())) {
                // Both empty - no partition clause
                return Collections.emptyList();
            }
            else if (from == null || from.trim().isEmpty()) {
                // Only upper bound: column <= to
                String partitionClause = JdbcSqlUtils.renderTemplate(
                        SynapseSqlUtils.getQueryFactory(),
                        "partition_to_clause",
                        Map.of("partitionColumn", column,
                                "partitionBoundaryTo", to));
                return Collections.singletonList(partitionClause);
            }
            else {
                // Only lower bound: column > from
                String partitionClause = JdbcSqlUtils.renderTemplate(
                        SynapseSqlUtils.getQueryFactory(),
                        "partition_from_clause",
                        Map.of("partitionColumn", column,
                                "partitionBoundaryFrom", from));
                return Collections.singletonList(partitionClause);
            }
        }
        else {
            LOGGER.debug("Fetching data without Partition");
        }
        return Collections.emptyList();
    }
    
    @Override
    public SynapseQueryBuilder withLimitClause(Constraints constraints)
    {
        // Azure Synapse doesn't support LIMIT clause, return empty string
        this.limitClause = "";
        return this;
    }
}
