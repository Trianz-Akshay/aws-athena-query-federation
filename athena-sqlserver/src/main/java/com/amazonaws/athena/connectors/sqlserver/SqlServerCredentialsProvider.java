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

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.utils.Validate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * SQL Server OAuth credentials provider that manages OAuth token lifecycle.
 * This provider handles token refresh, expiration, and provides credential properties
 * for SQL Server OAuth connections using Microsoft Entra ID with client_credentials grant type.
 */
public class SqlServerCredentialsProvider implements CredentialsProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlServerCredentialsProvider.class);
    
    private static final String ACCESS_TOKEN = "access_token";
    private static final String FETCHED_AT = "fetched_at";
    private static final String EXPIRES_IN = "expires_in";
    private static final String CLIENT_ID = "client_id";
    private static final String CLIENT_SECRET = "client_secret";
    private static final String TENANT_ID = "tenant_id";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    
    private static final String MICROSOFT_TOKEN_ENDPOINT = "https://login.microsoftonline.com/%s/oauth2/v2.0/token";
    private static final String CLIENT_CREDENTIALS_GRANT = "client_credentials";
    private static final String SCOPE = "https://database.windows.net/.default";

    private final String oauthSecretName;
    private final CachableSecretsManager secretsManager;
    private final ObjectMapper objectMapper;

    public SqlServerCredentialsProvider(String oauthSecretName)
    {
        this(oauthSecretName, SecretsManagerClient.create());
    }

    @VisibleForTesting
    public SqlServerCredentialsProvider(String oauthSecretName, SecretsManagerClient secretsClient)
    {
        this.oauthSecretName = Validate.notNull(oauthSecretName, "oauthSecretName must not be null");
        this.secretsManager = new CachableSecretsManager(secretsClient);
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public DefaultCredentials getCredential()
    {
        LOGGER.debug("Getting SQL Server credentials from secret: {}", oauthSecretName);
        Map<String, String> credentialMap = getCredentialMap();
        String username = credentialMap.get(USER);
        String password = credentialMap.get(PASSWORD);
        
        LOGGER.debug("Retrieved credentials - Username: {}, Password length: {}, Authentication: {}", 
            username, password != null ? password.length() : 0, credentialMap.get("authentication"));
        
        return new DefaultCredentials(username, password);
    }

    @Override
    public Map<String, String> getCredentialMap()
    {
        try {
            LOGGER.debug("Retrieving credential map from secret: {}", oauthSecretName);
            String secretString = secretsManager.getSecret(oauthSecretName);
            Map<String, String> oauthConfig = objectMapper.readValue(secretString, Map.class);
            
            LOGGER.debug("Retrieved OAuth config with keys: {}", oauthConfig.keySet());
            
            if (oauthConfig.containsKey(CLIENT_ID) && !oauthConfig.get(CLIENT_ID).isEmpty()) {
                // OAuth flow
                LOGGER.info("OAuth flow detected - using client_credentials grant type");
                String accessToken = fetchAccessTokenFromSecret(oauthConfig);
                
                Map<String, String> credentialMap = new HashMap<>();
                credentialMap.put(USER, oauthConfig.get(SqlServerConstants.USERNAME));
                credentialMap.put(PASSWORD, accessToken);
                credentialMap.put("authentication", "ActiveDirectoryServicePrincipal");
                
                LOGGER.info("OAuth credentials prepared successfully for user: {}", oauthConfig.get(SqlServerConstants.USERNAME));
                return credentialMap;
            }
            else {
                // Fallback to standard credentials
                LOGGER.info("Standard credentials flow detected - using username/password authentication");
                return Map.of(
                        USER, oauthConfig.get(SqlServerConstants.USERNAME),
                        PASSWORD, oauthConfig.get(SqlServerConstants.PASSWORD)
                );
            }
        }
        catch (Exception ex) {
            LOGGER.error("Error retrieving SQL Server credentials from secret {}: {}", oauthSecretName, ex.getMessage(), ex);
            throw new RuntimeException("Error retrieving SQL Server credentials: " + ex.getMessage(), ex);
        }
    }

    private String loadTokenFromSecretsManager(Map<String, String> oauthConfig)
    {
        if (oauthConfig.containsKey(ACCESS_TOKEN)) {
            LOGGER.debug("Found existing access token in secret");
            return oauthConfig.get(ACCESS_TOKEN);
        }
        LOGGER.debug("No existing access token found in secret");
        return null;
    }

    private void saveTokenToSecretsManager(JSONObject tokenJson, Map<String, String> oauthConfig)
    {
        LOGGER.debug("Saving new access token to Secrets Manager");
        
        // Update token related fields
        long fetchedAt = System.currentTimeMillis() / 1000;
        tokenJson.put(FETCHED_AT, fetchedAt);
        oauthConfig.put(ACCESS_TOKEN, tokenJson.getString(ACCESS_TOKEN));
        oauthConfig.put(EXPIRES_IN, String.valueOf(tokenJson.getInt(EXPIRES_IN)));
        oauthConfig.put(FETCHED_AT, String.valueOf(fetchedAt));

        // Save updated secret
        secretsManager.getSecretsManager().putSecretValue(builder -> builder
                .secretId(this.oauthSecretName)
                .secretString(String.valueOf(new JSONObject(oauthConfig)))
                .build());
        
        LOGGER.info("Successfully saved new access token to Secrets Manager. Token expires in {} seconds", 
            tokenJson.getInt(EXPIRES_IN));
    }

    private String fetchAccessTokenFromSecret(Map<String, String> oauthConfig) throws Exception
    {
        String accessToken;
        String clientId = Validate.notNull(oauthConfig.get(CLIENT_ID), "Missing required property: client_id");
        String clientSecret = Validate.notNull(oauthConfig.get(CLIENT_SECRET), "Missing required property: client_secret");
        String tenantId = Validate.notNull(oauthConfig.get(TENANT_ID), "Missing required property: tenant_id");

        LOGGER.debug("Fetching access token for client_id: {}, tenant_id: {}", clientId, tenantId);

        accessToken = loadTokenFromSecretsManager(oauthConfig);

        if (accessToken == null) {
            LOGGER.info("First time authentication - fetching new access token using client_credentials");
            JSONObject tokenJson = getTokenFromClientCredentials(clientId, clientSecret, tenantId);
            saveTokenToSecretsManager(tokenJson, oauthConfig);
            accessToken = tokenJson.getString(ACCESS_TOKEN);
        }
        else {
            long expiresIn = Long.parseLong(oauthConfig.get(EXPIRES_IN));
            long fetchedAt = Long.parseLong(oauthConfig.getOrDefault(FETCHED_AT, String.valueOf(0L)));
            long now = System.currentTimeMillis() / 1000;
            long timeUntilExpiry = expiresIn - (now - fetchedAt);

            LOGGER.debug("Token age: {} seconds, expires in: {} seconds, time until expiry: {} seconds", 
                (now - fetchedAt), expiresIn, timeUntilExpiry);

            if (timeUntilExpiry > 60) {
                LOGGER.debug("Access token still valid for {} more seconds", timeUntilExpiry);
            }
            else {
                LOGGER.info("Access token expired or expiring soon ({} seconds remaining). Fetching new token...", timeUntilExpiry);
                JSONObject refreshed = getTokenFromClientCredentials(clientId, clientSecret, tenantId);
                saveTokenToSecretsManager(refreshed, oauthConfig);
                accessToken = refreshed.getString(ACCESS_TOKEN);
            }
        }
        return accessToken;
    }

    private JSONObject getTokenFromClientCredentials(String clientId, String clientSecret, String tenantId) throws Exception
    {
        String tokenEndpoint = String.format(MICROSOFT_TOKEN_ENDPOINT, tenantId);
        LOGGER.debug("Requesting token from endpoint: {}", tokenEndpoint);
        
        String body = "grant_type=" + CLIENT_CREDENTIALS_GRANT
                + "&client_id=" + URLEncoder.encode(clientId, StandardCharsets.UTF_8)
                + "&client_secret=" + URLEncoder.encode(clientSecret, StandardCharsets.UTF_8)
                + "&scope=" + URLEncoder.encode(SCOPE, StandardCharsets.UTF_8);

        return requestToken(body, tokenEndpoint);
    }

    private JSONObject requestToken(String requestBody, String tokenEndpoint) throws Exception
    {
        LOGGER.debug("Making HTTP request to token endpoint");
        HttpURLConnection conn = getHttpURLConnection(tokenEndpoint);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(requestBody.getBytes(StandardCharsets.UTF_8));
        }

        int responseCode = conn.getResponseCode();
        LOGGER.debug("Token endpoint response code: {}", responseCode);
        
        InputStream is = (responseCode >= 200 && responseCode < 300) ?
                conn.getInputStream() : conn.getErrorStream();

        String response = new BufferedReader(new InputStreamReader(is))
                .lines()
                .reduce("", (acc, line) -> acc + line);

        if (responseCode != 200) {
            LOGGER.error("Token request failed with response code: {} and response: {}", responseCode, response);
            throw new RuntimeException("Failed: " + responseCode + " - " + response);
        }

        JSONObject tokenJson = new JSONObject(response);
        long fetchedAt = System.currentTimeMillis() / 1000;
        tokenJson.put(FETCHED_AT, fetchedAt);
        
        LOGGER.info("Successfully obtained access token. Expires in: {} seconds", tokenJson.getInt(EXPIRES_IN));
        return tokenJson;
    }

    static HttpURLConnection getHttpURLConnection(String tokenEndpoint) throws IOException
    {
        LOGGER.debug("Creating HTTP connection to: {}", tokenEndpoint);
        URL url = new URL(tokenEndpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setDoOutput(true);
        return conn;
    }
} 