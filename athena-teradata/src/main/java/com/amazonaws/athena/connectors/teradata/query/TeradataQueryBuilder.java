/*-
 * #%L
 * athena-teradata
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
package com.amazonaws.athena.connectors.teradata.query;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcPredicateBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcQueryBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSqlUtils;
import com.amazonaws.athena.connectors.teradata.TeradataSqlUtils;
import org.stringtemplate.v4.ST;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.teradata.TeradataConstants.TERADATA_QUOTE_CHARACTER;

public class TeradataQueryBuilder extends JdbcQueryBuilder<TeradataQueryBuilder>
{
    /** Partition column name used in Teradata table layout (must match TeradataMetadataHandler.BLOCK_PARTITION_COLUMN_NAME). */
    private static final String PARTITION_COLUMN_NAME = "partition";

    public TeradataQueryBuilder(ST template)
    {
        super(template, TERADATA_QUOTE_CHARACTER);
    }

    @Override
    protected JdbcPredicateBuilder createPredicateBuilder()
    {
        return new TeradataPredicateBuilder();
    }

    @Override
    protected List<String> getPartitionWhereClauses(Split split)
    {
        String partitionValue = split.getProperty(PARTITION_COLUMN_NAME);
        if (partitionValue != null && !"*".equals(partitionValue)) {
            return Collections.singletonList(JdbcSqlUtils.renderTemplate(
                    TeradataSqlUtils.getQueryFactory(),
                    "partition_clause",
                    Map.of("columnName", PARTITION_COLUMN_NAME, "value", partitionValue)));
        }
        return Collections.emptyList();
    }

    @Override
    public TeradataQueryBuilder withLimitClause(Constraints constraints)
    {
        // Teradata does not support LIMIT clause
        this.limitClause = "";
        return this;
    }
}
