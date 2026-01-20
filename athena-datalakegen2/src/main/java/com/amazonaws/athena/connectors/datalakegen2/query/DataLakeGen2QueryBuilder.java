/*-
 * #%L
 * athena-datalakegen2
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connectors.datalakegen2.query;

import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.datalakegen2.DataLakeGen2Constants;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcPredicateBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcQueryBuilder;
import org.stringtemplate.v4.ST;

public class DataLakeGen2QueryBuilder extends JdbcQueryBuilder<DataLakeGen2QueryBuilder>
{
    public DataLakeGen2QueryBuilder(ST template)
    {
        super(template, DataLakeGen2Constants.QUOTE_CHARACTER);
    }
    
    @Override
    protected JdbcPredicateBuilder createPredicateBuilder()
    {
        return new DataLakeGen2PredicateBuilder();
    }

    @Override
    public DataLakeGen2QueryBuilder withLimitClause(Constraints constraints)
    {
        // DataLakeGen2 does not support LIMIT clause, return empty string
        this.limitClause = "";
        return this;
    }
}
