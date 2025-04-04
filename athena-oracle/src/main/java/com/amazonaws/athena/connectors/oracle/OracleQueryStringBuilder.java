/*-
 * #%L
 * athena-oracle
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.base.Strings;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Extends {@link JdbcSplitQueryBuilder} and implements ORACLE specific SQL clauses for split.
 *
 * Oracle provides named partitions which can be used in a FROM clause.
 */
public class OracleQueryStringBuilder
        extends JdbcSplitQueryBuilder
{
    public OracleQueryStringBuilder(final String quoteCharacter, final FederationExpressionParser federationExpressionParser)
    {
        super(quoteCharacter, federationExpressionParser);
    }

    @Override
    protected String getFromClauseWithSplit(String catalog, String schema, String table, Split split)
    {
        StringBuilder tableName = new StringBuilder();
        if (!Strings.isNullOrEmpty(catalog)) {
            tableName.append(quote(catalog)).append('.');
        }
        if (!Strings.isNullOrEmpty(schema)) {
            tableName.append(quote(schema)).append('.');
        }
        tableName.append(quote(table));

        String partitionName = split.getProperty(OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME);

        if (OracleMetadataHandler.ALL_PARTITIONS.equals(partitionName)) {
            // No partitions
            return String.format(" FROM %s ", tableName);
        }

        Set<String> partitionVals = split.getProperties().keySet();
        String partValue = split.getProperty(partitionVals.iterator().next());
        return String.format(" FROM %s ", tableName + " " + "PARTITION " + "(" + partValue + ")");
    }

    @Override
    protected List<String> getPartitionWhereClauses(final Split split)
    {
        return Collections.emptyList();
    }

    //Returning empty string as Oracle does not support LIMIT clause
    @Override
    protected String appendLimitOffset(Split split, Constraints constraints)
    {
        return String.format(" FETCH FIRST %d ROWS ONLY ", constraints.getLimit());
    }
}
