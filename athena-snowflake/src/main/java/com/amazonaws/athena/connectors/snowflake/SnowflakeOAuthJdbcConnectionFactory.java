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

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.utils.Validate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.regex.Matcher;

public class SnowflakeOAuthJdbcConnectionFactory extends GenericJdbcConnectionFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeOAuthJdbcConnectionFactory.class);
    
    private final DatabaseConnectionInfo databaseConnectionInfo;
    private final DatabaseConnectionConfig databaseConnectionConfig;
    private final Properties jdbcProperties;
    private final String oauthSecretName;
    private final Map<String, String> configOptions;
    private final SecretsManagerClient secretsClient;
    private final SnowflakeOAuthTokenManager tokenManager;
    
    // Cache of token managers per secret name to avoid creating multiple instances
    private static final Map<String, SnowflakeOAuthTokenManager> TOKEN_MANAGERS = new ConcurrentHashMap<>();

    public SnowflakeOAuthJdbcConnectionFactory(DatabaseConnectionConfig databaseConnectionConfig,
                                               Map<String, String> properties,
                                               DatabaseConnectionInfo databaseConnectionInfo, 
                                               Map<String, String> configOptions)
    {
        super(databaseConnectionConfig, properties, databaseConnectionInfo);
        this.databaseConnectionInfo = Validate.notNull(databaseConnectionInfo, "databaseConnectionInfo must not be null");
        this.databaseConnectionConfig = Validate.notNull(databaseConnectionConfig, "databaseConnectionConfig must not be null");
        this.configOptions = configOptions;
        this.jdbcProperties = new Properties();
        if (properties != null) {
            this.jdbcProperties.putAll(properties);
        }
        this.secretsClient = SecretsManagerClient.create();
        this.oauthSecretName = Validate.notNull(databaseConnectionConfig.getSecret(), "Missing required property: secret name");
        this.tokenManager = TOKEN_MANAGERS.computeIfAbsent(oauthSecretName, 
            secretName -> new SnowflakeOAuthTokenManager(secretsClient, secretName));
    }

    @VisibleForTesting
    public SnowflakeOAuthJdbcConnectionFactory(DatabaseConnectionConfig databaseConnectionConfig,
                                               Map<String, String> properties,
                                               DatabaseConnectionInfo databaseConnectionInfo, 
                                               Map<String, String> configOptions,
                                               SecretsManagerClient secretsClient)
    {
        super(databaseConnectionConfig, properties, databaseConnectionInfo);
        this.databaseConnectionInfo = Validate.notNull(databaseConnectionInfo, "databaseConnectionInfo must not be null");
        this.databaseConnectionConfig = Validate.notNull(databaseConnectionConfig, "databaseConnectionConfig must not be null");
        this.configOptions = configOptions;
        this.jdbcProperties = new Properties();
        if (properties != null) {
            this.jdbcProperties.putAll(properties);
        }
        this.secretsClient = secretsClient;
        this.oauthSecretName = Validate.notNull(databaseConnectionConfig.getSecret(), "Missing required property: secret name");
        this.tokenManager = TOKEN_MANAGERS.computeIfAbsent(oauthSecretName, 
            secretName -> new SnowflakeOAuthTokenManager(secretsClient, secretName));
    }

    @Override
    public Connection getConnection(final CredentialsProvider credentialsProvider)
    {
        try {
            // Check if OAuth is configured by looking for auth_code in the secret
            if (isOAuthConfigured()) {
                if (credentialsProvider != null) {
                    LOGGER.info("Creating OAuth-enabled Snowflake connection with automatic token refresh");
                    
                    // Create connection supplier for reconnection
                    Supplier<Connection> connectionSupplier = () -> {
                        try {
                            return createRawConnection(credentialsProvider);
                        } 
                        catch (Exception e) {
                            throw new RuntimeException("Failed to create connection", e);
                        }
                    };
                    
                    // Create initial connection
                    Connection rawConnection = createRawConnection(credentialsProvider);
                    
                    // Wrap with OAuth-aware connection that handles token expiration
                    return new SnowflakeOAuthConnection(rawConnection, connectionSupplier, tokenManager);
                }
            } 
            else {
                // Fall back to standard connection for non-OAuth configurations
                LOGGER.info("OAuth not configured, falling back to standard connection");
                return super.getConnection(credentialsProvider);
            }
        } 
        catch (Exception ex) {
            throw new RuntimeException("Error creating Snowflake connection: " + ex.getMessage(), ex);
        }
        return null;
    }
    
    /**
     * Checks if OAuth is configured by verifying the presence of auth_code in the secret
     */
    private boolean isOAuthConfigured()
    {
        try {
            // This will be cached by the token manager, so it's efficient to call
            String accessToken = tokenManager.getValidAccessToken();
            return accessToken != null;
        } 
        catch (Exception e) {
            LOGGER.debug("OAuth not configured or failed to get token: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Creates a raw JDBC connection with OAuth authentication
     */
    private Connection createRawConnection(CredentialsProvider credentialsProvider) throws Exception
    {
        final String derivedJdbcString;
        Matcher secretMatcher = SECRET_NAME_PATTERN.matcher(databaseConnectionConfig.getJdbcConnectionString());
        derivedJdbcString = secretMatcher.replaceAll(Matcher.quoteReplacement(""));

        // Create a copy of JDBC properties to avoid modifying the shared instance
        Properties connectionProperties = new Properties();
        connectionProperties.putAll(jdbcProperties);
        
        connectionProperties.put("user", credentialsProvider.getCredential().getUser());
        
        // Get valid access token (will refresh if needed)
        String accessToken = tokenManager.getValidAccessToken();
        connectionProperties.put("password", accessToken);
        connectionProperties.put("authenticator", "oauth");
        
        // Add connection timeout and other resilience properties
        connectionProperties.put("loginTimeout", "30");
        connectionProperties.put("networkTimeout", "30000");
        connectionProperties.put("queryTimeout", "0"); // No query timeout, let Athena handle it
        connectionProperties.put("retryCount", "3");
        connectionProperties.put("retryInterval", "1000");

        Class.forName(databaseConnectionInfo.getDriverClassName()).newInstance();

        LOGGER.debug("Creating Snowflake connection with OAuth token");
        return DriverManager.getConnection(derivedJdbcString, connectionProperties);
    }

    public String getS3ExportBucket()
    {
        return configOptions.get("spill_bucket");
    }
}
