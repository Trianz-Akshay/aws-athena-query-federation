/*-
 * #%L
 * Amazon Athena MySQL Connector
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
package com.amazonaws.athena.connectors.mysql.query;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcPredicateBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import com.amazonaws.athena.connectors.mysql.MySqlFederationExpressionParser;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.amazonaws.athena.connectors.mysql.MySqlConstants.MYSQL_QUOTE_CHARACTER;

public class MySqlPredicateBuilder extends JdbcPredicateBuilder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlPredicateBuilder.class);

    public MySqlPredicateBuilder()
    {
        super(MYSQL_QUOTE_CHARACTER, new MySqlQueryFactory());
    }

    @Override
    public List<String> buildConjuncts(List<Field> columns, Constraints constraints,
                                       List<TypeAndValue> parameterValues, Split split)
    {
        LOGGER.debug("Inside buildConjuncts(): ");
        List<String> builder = super.buildConjuncts(columns, constraints, parameterValues, split);

        // Add complex expressions (federation expressions)
        builder.addAll(new MySqlFederationExpressionParser(MYSQL_QUOTE_CHARACTER).parseComplexExpressions(columns, constraints, parameterValues));
        return builder;
    }
}
