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
package com.amazonaws.athena.connectors.oracle.query;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcPredicateBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcQueryBuilder;
import org.stringtemplate.v4.ST;

import static com.amazonaws.athena.connectors.oracle.OracleConstants.ALL_PARTITIONS;
import static com.amazonaws.athena.connectors.oracle.OracleConstants.BLOCK_PARTITION_COLUMN_NAME;
import static com.amazonaws.athena.connectors.oracle.OracleRecordHandler.ORACLE_QUOTE_CHARACTER;

public class OracleQueryBuilder extends JdbcQueryBuilder<OracleQueryBuilder>
{
    private String partitionClause;

    public OracleQueryBuilder(ST template)
    {
        super(template, ORACLE_QUOTE_CHARACTER);
    }

    @Override
    protected JdbcPredicateBuilder createPredicateBuilder()
    {
        return new OraclePredicateBuilder();
    }

    public OracleQueryBuilder withPartitionClause(Split split)
    {
        String partitionName = split.getProperty(BLOCK_PARTITION_COLUMN_NAME);

        if (ALL_PARTITIONS.equals(partitionName)) {
            // No partitions
            this.partitionClause = "";
        }
        else {
            java.util.Set<String> partitionVals = split.getProperties().keySet();
            if (!partitionVals.isEmpty()) {
                String partValue = split.getProperty(partitionVals.iterator().next());
                this.partitionClause = "PARTITION (" + partValue + ")";
            }
            else {
                this.partitionClause = "";
            }
        }
        return this;
    }

    public OracleQueryBuilder withLimitClause(Constraints constraints)
    {
        // Oracle uses FETCH FIRST n ROWS ONLY instead of LIMIT
        if (constraints.getLimit() > 0) {
            this.limitClause = String.format("FETCH FIRST %d ROWS ONLY", constraints.getLimit());
        }
        else {
            this.limitClause = "";
        }
        return this;
    }

    // Getter for StringTemplate access
    public String getPartitionClause()
    {
        return (partitionClause != null && !partitionClause.isEmpty()) ? partitionClause : null;
    }
}

