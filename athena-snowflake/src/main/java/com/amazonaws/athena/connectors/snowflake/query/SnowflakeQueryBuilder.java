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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcPredicateBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcQueryBuilder;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.stringtemplate.v4.ST;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connectors.snowflake.SnowflakeConstants.SNOWFLAKE_QUOTE_CHARACTER;

public class SnowflakeQueryBuilder extends JdbcQueryBuilder<SnowflakeQueryBuilder>
{
    private JdbcPredicateBuilder customPredicateBuilder;
    
    public SnowflakeQueryBuilder(ST template)
    {
        super(template, SNOWFLAKE_QUOTE_CHARACTER);
    }
    
    /**
     * Sets a custom predicate builder to use instead of the default one.
     * This allows using embedded value predicate builder for S3 export scenarios.
     * 
     * @param predicateBuilder The custom predicate builder to use
     * @return This builder instance for method chaining
     */
    public SnowflakeQueryBuilder withPredicateBuilder(JdbcPredicateBuilder predicateBuilder)
    {
        this.customPredicateBuilder = predicateBuilder;
        return this;
    }
    
    @Override
    protected JdbcPredicateBuilder createPredicateBuilder()
    {
        if (customPredicateBuilder != null) {
            return customPredicateBuilder;
        }
        return new SnowflakePredicateBuilder();
    }
    
    @Override
    protected List<String> getPartitionWhereClauses(Split split)
    {
        // Snowflake doesn't have partition-specific WHERE clauses
        return Collections.emptyList();
    }
    
    @Override
    public SnowflakeQueryBuilder withProjection(Schema schema, Split split)
    {
        // Handle null split for S3 export scenarios
        if (split == null) {
            this.projection = schema.getFields().stream()
                    .map(Field::getName)
                    .filter(name -> !name.equalsIgnoreCase("partition"))
                    .map(this::transformColumnForProjection)
                    .collect(Collectors.toList());
        }
        else {
            super.withProjection(schema, split);
        }
        return this;
    }
}
