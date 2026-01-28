/*-
 * #%L
 * athena-synapse
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
package com.amazonaws.athena.connectors.synapse.query;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

public class SynapseQueryFactoryTest
{
    private SynapseQueryFactory queryFactory;

    @Before
    public void setUp()
    {
        queryFactory = new SynapseQueryFactory();
    }

    @Test
    public void createQueryFactory_CreatesInstanceSuccessfully()
    {
        assertNotNull("Query factory should not be null", queryFactory);
    }

    @Test
    public void createQueryBuilder_ReturnsNonNullQueryBuilder()
    {
        SynapseQueryBuilder queryBuilder = queryFactory.createQueryBuilder();

        assertNotNull("Query builder should not be null", queryBuilder);
    }

    @Test
    public void createQueryBuilder_ReturnsNewInstanceEachTime()
    {
        SynapseQueryBuilder queryBuilder1 = queryFactory.createQueryBuilder();
        SynapseQueryBuilder queryBuilder2 = queryFactory.createQueryBuilder();

        assertNotNull("First query builder should not be null", queryBuilder1);
        assertNotNull("Second query builder should not be null", queryBuilder2);
        assertNotSame("Query builders should be different instances", queryBuilder1, queryBuilder2);
    }

    @Test
    public void queryFactory_CanBeReusedMultipleTimes()
    {
        // Verify factory can create multiple builders without issues
        for (int i = 0; i < 10; i++) {
            SynapseQueryBuilder queryBuilder = queryFactory.createQueryBuilder();
            assertNotNull("Query builder " + i + " should not be null", queryBuilder);
        }
    }
}
