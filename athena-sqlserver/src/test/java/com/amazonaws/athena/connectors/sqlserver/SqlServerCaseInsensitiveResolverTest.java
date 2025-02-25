/*-
 * #%L
 * athena-sqlserver
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
package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SqlServerCaseInsensitiveResolverTest
{

    private Connection mockConnection;
    private ResultSet mockResultSet;

    @Before
    public void setUp() throws SQLException
    {
        mockConnection = mock(Connection.class);
        PreparedStatement mockStmt = mock(PreparedStatement.class);
        mockResultSet = mock(ResultSet.class);

        when(mockConnection.prepareStatement(anyString())).thenReturn(mockStmt);
        when(mockStmt.executeQuery()).thenReturn(mockResultSet);
    }

    @Test
    public void getAdjustedTableObjectNameCaseInsensitively() throws SQLException
    {
        TableName inputTableName = new TableName("schema", "table");
        TableName expectedTableName = new TableName("SCHEMA", "TABLE");

        when(mockResultSet.next()).thenReturn(true, false);
        when(mockResultSet.getString("TABLE_SCHEMA")).thenReturn("SCHEMA");
        when(mockResultSet.getString("TABLE_NAME")).thenReturn("TABLE");

        Map<String, String> config = new HashMap<>();
        config.put("casing_mode", "CASE_INSENSITIVE_SEARCH");

        TableName result = SqlServerCaseInsensitiveResolver.getAdjustedTableObjectNameBasedOnConfig(mockConnection, inputTableName, config);
        Assert.assertEquals(expectedTableName, result);
    }

    @Test
    public void getAdjustedTableObjectNameNoConfig() throws SQLException
    {
        TableName inputTableName = new TableName("schema", "table");
        TableName result = SqlServerCaseInsensitiveResolver.getAdjustedTableObjectNameBasedOnConfig(mockConnection, inputTableName, Collections.emptyMap());
        Assert.assertEquals(inputTableName, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void getAdjustedTableObjectNameInvalidCasingMode() throws SQLException
    {
        TableName inputTableName = new TableName("schema", "table");
        Map<String, String> config = new HashMap<>();
        config.put("casing_mode", "INVALID_MODE");

        SqlServerCaseInsensitiveResolver.getAdjustedTableObjectNameBasedOnConfig(mockConnection, inputTableName, config);
    }

    @Test(expected = RuntimeException.class)
    public void getObjectNameCaseInsensitivelyMultiMatches() throws SQLException
    {
        TableName inputTableName = new TableName("schema", "table");

        when(mockResultSet.next()).thenReturn(true, true, false);
        when(mockResultSet.getString("TABLE_SCHEMA")).thenReturn("SCHEMA");
        when(mockResultSet.getString("TABLE_NAME")).thenReturn("TABLE");

        SqlServerCaseInsensitiveResolver.getObjectNameCaseInsensitively(mockConnection, inputTableName);
    }
}
