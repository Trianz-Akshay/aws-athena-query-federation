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
import com.amazonaws.athena.connectors.jdbc.manager.JdbcQueryBuilder;
import org.apache.arrow.vector.types.pojo.Schema;
import org.stringtemplate.v4.ST;

import java.util.List;

import static com.amazonaws.athena.connectors.sqlserver.SqlServerConstants.SQLSERVER_QUOTE_CHARACTER;

public class SqlServerQueryBuilder extends JdbcQueryBuilder<SqlServerQueryBuilder>
{
    public SqlServerQueryBuilder(ST template)
    {
        super(template, SQLSERVER_QUOTE_CHARACTER);
    }

    public SqlServerQueryBuilder withConjuncts(Schema schema, Constraints constraints, Split split)
    {
        this.conjuncts = new SqlServerPredicateBuilder().buildConjuncts(
                schema.getFields(), constraints, this.parameterValues, split);
        
        // Add partition clauses if applicable
        List<String> partitionClauses = getPartitionWhereClauses(split);
        if (!partitionClauses.isEmpty()) {
            this.conjuncts.addAll(partitionClauses);
        }
        return this;
    }
    
    private List<String> getPartitionWhereClauses(Split split)
    {
        String partitionFunction = split.getProperty("PARTITION_FUNCTION");
        String partitioningColumn = split.getProperty("PARTITIONING_COLUMN");
        String partitionNumber = split.getProperty("partition_number");

        if (partitionFunction != null && partitioningColumn != null && partitionNumber != null && !partitionNumber.equals("0")) {
            // Note: SQL Server $PARTITION function doesn't use quotes around column names
            // Format: $PARTITION.pf(testCol1) = 1 (with leading space to get " AND  $PARTITION" format)
            return java.util.Collections.singletonList(" $PARTITION." + partitionFunction + "(" + partitioningColumn + ") = " + partitionNumber);
        }
        return java.util.Collections.emptyList();
    }

    public SqlServerQueryBuilder withLimitClause()
    {
        // SQL Server doesn't support LIMIT clause, return empty string
        this.limitClause = "";
        return this;
    }
}
