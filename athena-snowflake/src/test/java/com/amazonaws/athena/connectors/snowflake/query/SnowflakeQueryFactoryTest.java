/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake.query;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SnowflakeQueryFactoryTest
{
    @Test
    public void constructor_WhenCalled_CreatesInstance()
    {
        SnowflakeQueryFactory factory = new SnowflakeQueryFactory();
        assertNotNull("Factory should not be null", factory);
    }

    @Test
    public void createQueryBuilder_WhenCalled_ReturnsSnowflakeQueryBuilder()
    {
        SnowflakeQueryFactory factory = new SnowflakeQueryFactory();
        SnowflakeQueryBuilder builder = factory.createQueryBuilder();
        
        assertNotNull("Query builder should not be null", builder);
        assertTrue("Query builder should be instance of SnowflakeQueryBuilder", 
                builder instanceof SnowflakeQueryBuilder);
    }

    @Test
    public void createQueryBuilder_WhenCalledMultipleTimes_ReturnsNewInstances()
    {
        SnowflakeQueryFactory factory = new SnowflakeQueryFactory();
        SnowflakeQueryBuilder builder1 = factory.createQueryBuilder();
        SnowflakeQueryBuilder builder2 = factory.createQueryBuilder();
        
        assertNotNull("First builder should not be null", builder1);
        assertNotNull("Second builder should not be null", builder2);
        assertTrue("Builders should be different instances", builder1 != builder2);
    }
}
