/*-
 * #%L
 * athena-sqlserver
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcQueryFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSqlUtils;
import com.amazonaws.athena.connectors.jdbc.manager.TemplateBasedJdbcFederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.amazonaws.athena.connector.lambda.domain.predicate.expression.ConstantExpression.DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME;

/**
 * SQL Server implementation of FederationExpressionParser using StringTemplate.
 * Extends TemplateBasedJdbcFederationExpressionParser which provides the common
 * template-based implementation for all migrated JDBC connectors.
 * Overrides parseConstantExpression for SQL Server-specific date handling.
 */
public class SqlServerFederationExpressionParser extends TemplateBasedJdbcFederationExpressionParser
{
    private static final String STRING_LITERAL_QUOTE = "'";

    public SqlServerFederationExpressionParser(String quoteChar)
    {
        super(quoteChar);
    }

    @Override
    protected JdbcQueryFactory getQueryFactory()
    {
        return SqlServerSqlUtils.getQueryFactory();
    }

    /**
     * SQL Server-specific constant expression parsing.
     * Handles string quoting for Utf8, LargeUtf8, and DATEDAY types.
     */
    @Override
    public String parseConstantExpression(ConstantExpression constantExpression,
                                          List<TypeAndValue> parameterValues)
    {
        Block values = constantExpression.getValues();
        FieldReader fieldReader = values.getFieldReader(DEFAULT_CONSTANT_EXPRESSION_BLOCK_NAME);

        List<String> constants = new ArrayList<>();

        for (int i = 0; i < values.getRowCount(); i++) {
            fieldReader.setPosition(i);
            String strVal = BlockUtils.fieldToString(fieldReader);
            constants.add(strVal);
        }
        if (constantExpression.getType().equals(ArrowType.Utf8.INSTANCE)
                || constantExpression.getType().equals(ArrowType.LargeUtf8.INSTANCE)
                || fieldReader.getMinorType().equals(Types.MinorType.DATEDAY)) {
            constants = constants.stream()
                    .map(val -> STRING_LITERAL_QUOTE + val + STRING_LITERAL_QUOTE)
                    .collect(Collectors.toList());
        }

        return JdbcSqlUtils.renderTemplate(getQueryFactory(), "comma_separated_list", Map.of("items", constants));
    }
}
