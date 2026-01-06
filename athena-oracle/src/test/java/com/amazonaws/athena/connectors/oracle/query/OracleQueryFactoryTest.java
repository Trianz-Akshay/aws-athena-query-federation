package com.amazonaws.athena.connectors.oracle.query;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

/**
 * Unit tests for OracleQueryFactory.
 */
public class OracleQueryFactoryTest
{
    private OracleQueryFactory queryFactory;

    @Before
    public void setUp()
    {
        queryFactory = new OracleQueryFactory();
    }

    @Test
    public void constructor_WhenCalled_CreatesInstance()
    {
        OracleQueryFactory factory = new OracleQueryFactory();
        
        assertNotNull("Factory should not be null", factory);
    }

    @Test
    public void createQueryBuilder_WhenCalled_ReturnsOracleQueryBuilder()
    {
        OracleQueryBuilder builder = queryFactory.createQueryBuilder();
        
        assertNotNull("Query builder should not be null", builder);
    }

    @Test
    public void createQueryBuilder_WhenCalledMultipleTimes_ReturnsNewInstances()
    {
        OracleQueryBuilder builder1 = queryFactory.createQueryBuilder();
        OracleQueryBuilder builder2 = queryFactory.createQueryBuilder();
        
        assertNotNull("First builder should not be null", builder1);
        assertNotNull("Second builder should not be null", builder2);
        assertNotSame("Builders should be different instances", builder1, builder2);
    }

    @Test
    public void createQueryBuilder_WhenCalled_ReturnsProperlyInitializedBuilder()
    {
        OracleQueryBuilder builder = queryFactory.createQueryBuilder();
        
        assertNotNull("Query builder should not be null", builder);
        // Verify the builder is properly initialized by checking template name
        String templateName = OracleQueryBuilder.getTemplateName();
        assertNotNull("Template name should not be null", templateName);
        assertEquals("Template name should be select_query", "select_query", templateName);
        
        // Verify the builder has access to parameter values list (proves initialization)
        assertNotNull("Parameter values should not be null", builder.getParameterValues());
    }
}
