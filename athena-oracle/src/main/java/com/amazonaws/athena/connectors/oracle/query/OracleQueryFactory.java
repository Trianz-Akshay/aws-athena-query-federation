package com.amazonaws.athena.connectors.oracle.query;

import com.amazonaws.athena.connectors.jdbc.manager.JdbcQueryFactory;

/**
 * Factory for creating Oracle query builders with StringTemplate support.
 */
public class OracleQueryFactory extends JdbcQueryFactory
{
    private static final String TEMPLATE_FILE = "Oracle.stg";

    public OracleQueryFactory()
    {
        super(TEMPLATE_FILE);
    }

    public OracleQueryBuilder createQueryBuilder()
    {
        return new OracleQueryBuilder(getQueryTemplate(OracleQueryBuilder.getTemplateName()));
    }
}

