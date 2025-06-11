/*-
 * #%L
 * athena-snowflake
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
package com.amazonaws.athena.connectors.snowflake;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * A wrapper around JDBC Connection that handles OAuth token expiration and automatic reconnection
 */
public class SnowflakeOAuthConnection implements Connection
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeOAuthConnection.class);
    
    private volatile Connection delegate;
    private final Supplier<Connection> connectionSupplier;
    private final SnowflakeOAuthTokenManager tokenManager;
    private static final int MAX_RETRY_ATTEMPTS = 3;
    
    public SnowflakeOAuthConnection(Connection delegate, Supplier<Connection> connectionSupplier, SnowflakeOAuthTokenManager tokenManager)
    {
        this.delegate = delegate;
        this.connectionSupplier = connectionSupplier;
        this.tokenManager = tokenManager;
    }
    
    /**
     * Executes an operation with automatic retry on token expiration
     */
    private <T> T executeWithRetry(ConnectionOperation<T> operation) throws SQLException
    {
        SQLException lastException = null;
        
        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                return operation.execute(delegate);
            } 
            catch (SQLException e) {
                lastException = e;
                
                if (isTokenExpiredException(e) && attempt < MAX_RETRY_ATTEMPTS) {
                    LOGGER.warn("Token expired during operation (attempt {}), refreshing connection...", attempt);
                    try {
                        reconnectWithFreshToken();
                    } 
                    catch (Exception refreshException) {
                        LOGGER.error("Failed to refresh connection", refreshException);
                        throw new SQLException("Failed to refresh connection after token expiration", refreshException);
                    }
                } 
                else {
                    throw e;
                }
            }
        }
        
        throw lastException;
    }
    
    /**
     * Checks if the exception is related to token expiration
     */
    private boolean isTokenExpiredException(SQLException e)
    {
        String message = e.getMessage().toLowerCase();
        return message.contains("authentication") ||
               message.contains("token") ||
               message.contains("oauth") ||
               message.contains("expired") ||
               message.contains("unauthorized") ||
               e.getSQLState() != null && e.getSQLState().startsWith("28"); // Authentication failure SQL state
    }
    
    /**
     * Reconnects with a fresh token
     */
    private void reconnectWithFreshToken() throws Exception
    {
        LOGGER.info("Reconnecting with fresh OAuth token...");
        
        // Force token refresh
        tokenManager.forceTokenRefresh();
        
        // Close existing connection
        try {
            if (delegate != null && !delegate.isClosed()) {
                delegate.close();
            }
        } 
        catch (SQLException e) {
            LOGGER.warn("Exception while closing old connection", e);
        }
        
        // Create new connection
        delegate = connectionSupplier.get();
        LOGGER.info("Successfully reconnected with fresh token");
    }
    
    @FunctionalInterface
    private interface ConnectionOperation<T>
    {
        T execute(Connection connection) throws SQLException;
    }
    
    // Delegate all Connection methods with retry logic where appropriate
    
    @Override
    public Statement createStatement() throws SQLException
    {
        return executeWithRetry(Connection::createStatement);
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException
    {
        return executeWithRetry(conn -> conn.prepareStatement(sql));
    }
    
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException
    {
        return executeWithRetry(conn -> conn.prepareCall(sql));
    }
    
    @Override
    public String nativeSQL(String sql) throws SQLException
    {
        return executeWithRetry(conn -> conn.nativeSQL(sql));
    }
    
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        executeWithRetry(conn -> {
            conn.setAutoCommit(autoCommit);
            return null;
        });
    }
    
    @Override
    public boolean getAutoCommit() throws SQLException
    {
        return executeWithRetry(Connection::getAutoCommit);
    }
    
    @Override
    public void commit() throws SQLException
    {
        executeWithRetry(conn -> {
            conn.commit();
            return null;
        });
    }
    
    @Override
    public void rollback() throws SQLException
    {
        executeWithRetry(conn -> {
            conn.rollback();
            return null;
        });
    }
    
    @Override
    public void close() throws SQLException
    {
        if (delegate != null) {
            delegate.close();
        }
    }
    
    @Override
    public boolean isClosed() throws SQLException
    {
        return delegate == null || delegate.isClosed();
    }
    
    @Override
    public DatabaseMetaData getMetaData() throws SQLException
    {
        return executeWithRetry(Connection::getMetaData);
    }
    
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException
    {
        executeWithRetry(conn -> {
            conn.setReadOnly(readOnly);
            return null;
        });
    }
    
    @Override
    public boolean isReadOnly() throws SQLException
    {
        return executeWithRetry(Connection::isReadOnly);
    }
    
    @Override
    public void setCatalog(String catalog) throws SQLException
    {
        executeWithRetry(conn -> {
            conn.setCatalog(catalog);
            return null;
        });
    }
    
    @Override
    public String getCatalog() throws SQLException
    {
        return executeWithRetry(Connection::getCatalog);
    }
    
    @Override
    public void setTransactionIsolation(int level) throws SQLException
    {
        executeWithRetry(conn -> {
            conn.setTransactionIsolation(level);
            return null;
        });
    }
    
    @Override
    public int getTransactionIsolation() throws SQLException
    {
        return executeWithRetry(Connection::getTransactionIsolation);
    }
    
    @Override
    public SQLWarning getWarnings() throws SQLException
    {
        return executeWithRetry(Connection::getWarnings);
    }
    
    @Override
    public void clearWarnings() throws SQLException
    {
        executeWithRetry(conn -> {
            conn.clearWarnings();
            return null;
        });
    }
    
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
    {
        return executeWithRetry(conn -> conn.createStatement(resultSetType, resultSetConcurrency));
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
    {
        return executeWithRetry(conn -> conn.prepareStatement(sql, resultSetType, resultSetConcurrency));
    }
    
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
    {
        return executeWithRetry(conn -> conn.prepareCall(sql, resultSetType, resultSetConcurrency));
    }
    
    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException
    {
        return executeWithRetry(Connection::getTypeMap);
    }
    
    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException
    {
        executeWithRetry(conn -> {
            conn.setTypeMap(map);
            return null;
        });
    }
    
    @Override
    public void setHoldability(int holdability) throws SQLException
    {
        executeWithRetry(conn -> {
            conn.setHoldability(holdability);
            return null;
        });
    }
    
    @Override
    public int getHoldability() throws SQLException
    {
        return executeWithRetry(Connection::getHoldability);
    }
    
    @Override
    public Savepoint setSavepoint() throws SQLException
    {
        return executeWithRetry(Connection::setSavepoint);
    }
    
    @Override
    public Savepoint setSavepoint(String name) throws SQLException
    {
        return executeWithRetry(conn -> conn.setSavepoint(name));
    }
    
    @Override
    public void rollback(Savepoint savepoint) throws SQLException
    {
        executeWithRetry(conn -> {
            conn.rollback(savepoint);
            return null;
        });
    }
    
    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException
    {
        executeWithRetry(conn -> {
            conn.releaseSavepoint(savepoint);
            return null;
        });
    }
    
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        return executeWithRetry(conn -> conn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability));
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        return executeWithRetry(conn -> conn.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
    }
    
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException
    {
        return executeWithRetry(conn -> conn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
    {
        return executeWithRetry(conn -> conn.prepareStatement(sql, autoGeneratedKeys));
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
    {
        return executeWithRetry(conn -> conn.prepareStatement(sql, columnIndexes));
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
    {
        return executeWithRetry(conn -> conn.prepareStatement(sql, columnNames));
    }
    
    @Override
    public Clob createClob() throws SQLException
    {
        return executeWithRetry(Connection::createClob);
    }
    
    @Override
    public Blob createBlob() throws SQLException
    {
        return executeWithRetry(Connection::createBlob);
    }
    
    @Override
    public NClob createNClob() throws SQLException
    {
        return executeWithRetry(Connection::createNClob);
    }
    
    @Override
    public SQLXML createSQLXML() throws SQLException
    {
        return executeWithRetry(Connection::createSQLXML);
    }
    
    @Override
    public boolean isValid(int timeout) throws SQLException
    {
        return executeWithRetry(conn -> conn.isValid(timeout));
    }
    
    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException
    {
        try {
            executeWithRetry(conn -> {
                conn.setClientInfo(name, value);
                return null;
            });
        } 
        catch (SQLException e) {
            throw new SQLClientInfoException("Failed to set client info", null, e);
        }
    }
    
    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException
    {
        try {
            executeWithRetry(conn -> {
                conn.setClientInfo(properties);
                return null;
            });
        } 
        catch (SQLException e) {
            throw new SQLClientInfoException("Failed to set client info", null, e);
        }
    }
    
    @Override
    public String getClientInfo(String name) throws SQLException
    {
        return executeWithRetry(conn -> conn.getClientInfo(name));
    }
    
    @Override
    public Properties getClientInfo() throws SQLException
    {
        return executeWithRetry(Connection::getClientInfo);
    }
    
    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException
    {
        return executeWithRetry(conn -> conn.createArrayOf(typeName, elements));
    }
    
    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException
    {
        return executeWithRetry(conn -> conn.createStruct(typeName, attributes));
    }
    
    @Override
    public void setSchema(String schema) throws SQLException
    {
        executeWithRetry(conn -> {
            conn.setSchema(schema);
            return null;
        });
    }
    
    @Override
    public String getSchema() throws SQLException
    {
        return executeWithRetry(Connection::getSchema);
    }
    
    @Override
    public void abort(Executor executor) throws SQLException
    {
        executeWithRetry(conn -> {
            conn.abort(executor);
            return null;
        });
    }
    
    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException
    {
        executeWithRetry(conn -> {
            conn.setNetworkTimeout(executor, milliseconds);
            return null;
        });
    }
    
    @Override
    public int getNetworkTimeout() throws SQLException
    {
        return executeWithRetry(Connection::getNetworkTimeout);
    }
    
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        return executeWithRetry(conn -> conn.unwrap(iface));
    }
    
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
        return executeWithRetry(conn -> conn.isWrapperFor(iface));
    }
}
