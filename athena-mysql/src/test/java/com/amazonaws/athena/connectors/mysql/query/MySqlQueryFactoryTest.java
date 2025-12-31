package com.amazonaws.athena.connectors.mysql.query;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

/**
 * Unit tests for MySqlQueryFactory.
 */
public class MySqlQueryFactoryTest
{
    private MySqlQueryFactory queryFactory;

    @Before
    public void setUp()
    {
        queryFactory = new MySqlQueryFactory();
    }

    @Test
    public void constructor_WhenCalled_CreatesInstance()
    {
        MySqlQueryFactory factory = new MySqlQueryFactory();
        
        assertNotNull("Factory should not be null", factory);
    }

    @Test
    public void createQueryBuilder_WhenCalled_ReturnsMySqlQueryBuilder()
    {
        MySqlQueryBuilder builder = queryFactory.createQueryBuilder();
        
        assertNotNull("Query builder should not be null", builder);
    }

    @Test
    public void createQueryBuilder_WhenCalledMultipleTimes_ReturnsNewInstances()
    {
        MySqlQueryBuilder builder1 = queryFactory.createQueryBuilder();
        MySqlQueryBuilder builder2 = queryFactory.createQueryBuilder();
        
        assertNotNull("First builder should not be null", builder1);
        assertNotNull("Second builder should not be null", builder2);
        assertNotSame("Builders should be different instances", builder1, builder2);
    }

    @Test
    public void createQueryBuilder_WhenCalled_ReturnsProperlyInitializedBuilder()
    {
        MySqlQueryBuilder builder = queryFactory.createQueryBuilder();
        
        assertNotNull("Query builder should not be null", builder);
        // Verify the builder is properly initialized by checking template name
        String templateName = MySqlQueryBuilder.getTemplateName();
        assertNotNull("Template name should not be null", templateName);
        assertEquals("Template name should be select_query", "select_query", templateName);
        
        // Verify the builder has access to parameter values list (proves initialization)
        assertNotNull("Parameter values should not be null", builder.getParameterValues());
    }
}
