package com.amazonaws.athena.connectors.oracle.query;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcPredicateBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcQueryBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSqlUtils;
import com.amazonaws.athena.connectors.oracle.OracleSqlUtils;
import org.stringtemplate.v4.ST;

import java.util.Map;

import static com.amazonaws.athena.connectors.oracle.OracleMetadataHandler.ALL_PARTITIONS;
import static com.amazonaws.athena.connectors.oracle.OracleMetadataHandler.BLOCK_PARTITION_COLUMN_NAME;
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
                this.partitionClause = JdbcSqlUtils.renderTemplate(
                        OracleSqlUtils.getQueryFactory(),
                        "partition_clause",
                        Map.of("partitionValue", partValue)
                );
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

