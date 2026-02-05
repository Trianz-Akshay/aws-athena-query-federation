/*-
 * #%L
 * athena-teradata
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
package com.amazonaws.athena.connectors.teradata.query;

import com.amazonaws.athena.connectors.jdbc.manager.JdbcQueryFactory;

/**
 * Factory for creating Teradata query builders with StringTemplate support.
 */
public class TeradataQueryFactory extends JdbcQueryFactory
{
    private static final String TEMPLATE_FILE = "Teradata.stg";

    public TeradataQueryFactory()
    {
        super(TEMPLATE_FILE);
    }

    public TeradataQueryBuilder createQueryBuilder()
    {
        return new TeradataQueryBuilder(getQueryTemplate(TeradataQueryBuilder.getTemplateName()));
    }
}
