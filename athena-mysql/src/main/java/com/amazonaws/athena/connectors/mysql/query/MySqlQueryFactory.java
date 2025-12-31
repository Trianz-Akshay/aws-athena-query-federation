package com.amazonaws.athena.connectors.mysql.query;

import com.amazonaws.athena.connectors.jdbc.manager.JdbcQueryFactory;

/**
 * Factory for creating MySQL query builders with StringTemplate support.
 */
public class MySqlQueryFactory extends JdbcQueryFactory
{
    private static final String TEMPLATE_FILE = "MySql.stg";

    public MySqlQueryFactory()
    {
        super(TEMPLATE_FILE);
    }

    public MySqlQueryBuilder createQueryBuilder()
    {
        return new MySqlQueryBuilder(getQueryTemplate(MySqlQueryBuilder.getTemplateName()));
    }
}

