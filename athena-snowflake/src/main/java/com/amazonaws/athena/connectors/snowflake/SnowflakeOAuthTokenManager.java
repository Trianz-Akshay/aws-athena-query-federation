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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
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
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SnowflakeOAuthTokenManager
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeOAuthTokenManager.class);
    public static final String ACCESS_TOKEN = "access_token";
    public static final String FETCHED_AT = "fetched_at";
    public static final String REFRESH_TOKEN = "refresh_token";
    public static final String EXPIRES_IN = "expires_in";
    
    private final SecretsManagerClient secretsClient;
    private final String oauthSecretName;
    private final ReentrantReadWriteLock tokenLock = new ReentrantReadWriteLock();
    
    // Token refresh buffer - refresh token 2 minutes before expiration
    private static final long TOKEN_REFRESH_BUFFER_SECONDS = 120;
    
    public SnowflakeOAuthTokenManager(SecretsManagerClient secretsClient, String oauthSecretName)
    {
        this.secretsClient = Validate.notNull(secretsClient, "secretsClient must not be null");
        this.oauthSecretName = Validate.notNull(oauthSecretName, "oauthSecretName must not be null");
    }
    
    /**
     * Gets a valid access token, refreshing if necessary
     */
    public String getValidAccessToken() throws Exception
    {
        tokenLock.readLock().lock();
        try {
            Map<String, String> oauthConfig = loadOAuthConfig();
            String accessToken = oauthConfig.get(ACCESS_TOKEN);
            
            if (accessToken == null || isTokenExpired(oauthConfig)) {
                tokenLock.readLock().unlock();
                tokenLock.writeLock().lock();
                try {
                    // Double-check after acquiring write lock
                    oauthConfig = loadOAuthConfig();
                    accessToken = oauthConfig.get(ACCESS_TOKEN);
                    
                    if (accessToken == null || isTokenExpired(oauthConfig)) {
                        accessToken = refreshToken(oauthConfig);
                    }
                } 
                finally {
                    tokenLock.readLock().lock();
                    tokenLock.writeLock().unlock();
                }
            }
            
            return accessToken;
        } 
        finally {
            tokenLock.readLock().unlock();
        }
    }
    
    /**
     * Checks if the current token is expired or will expire soon
     */
    public boolean isTokenExpired(Map<String, String> oauthConfig)
    {
        if (!oauthConfig.containsKey(ACCESS_TOKEN) || !oauthConfig.containsKey(EXPIRES_IN) || !oauthConfig.containsKey(FETCHED_AT)) {
            return true;
        }
        
        long expiresIn = Long.parseLong(oauthConfig.get(EXPIRES_IN));
        long fetchedAt = Long.parseLong(oauthConfig.get(FETCHED_AT));
        long now = System.currentTimeMillis() / 1000;
        
        // Consider token expired if it will expire within the buffer time
        return (now - fetchedAt) >= (expiresIn - TOKEN_REFRESH_BUFFER_SECONDS);
    }
    
    /**
     * Forces a token refresh
     */
    public String forceTokenRefresh() throws Exception
    {
        tokenLock.writeLock().lock();
        try {
            Map<String, String> oauthConfig = loadOAuthConfig();
            return refreshToken(oauthConfig);
        } 
        finally {
            tokenLock.writeLock().unlock();
        }
    }
    
    private Map<String, String> loadOAuthConfig() throws Exception
    {
        GetSecretValueRequest getSecretValueRequest = GetSecretValueRequest.builder()
                .secretId(this.oauthSecretName)
                .build();
        
        GetSecretValueResponse secretValue = this.secretsClient.getSecretValue(getSecretValueRequest);
        return new ObjectMapper().readValue(secretValue.secretString(), Map.class);
    }
    
    private String refreshToken(Map<String, String> oauthConfig) throws Exception
    {
        String clientId = Validate.notNull(oauthConfig.get(SnowflakeConstants.CLIENT_ID), "Missing required property: client_id");
        String tokenEndpoint = Validate.notNull(oauthConfig.get(SnowflakeConstants.TOKEN_URL), "Missing required property: token_url");
        String clientSecret = Validate.notNull(oauthConfig.get(SnowflakeConstants.CLIENT_SECRET), "Missing required property: client_secret");
        
        String accessToken = oauthConfig.get(ACCESS_TOKEN);
        
        if (accessToken == null) {
            // First time - use authorization code
            String authCode = Validate.notNull(oauthConfig.get(SnowflakeConstants.AUTH_CODE), "Missing required property: auth_code");
            String redirectUri = Validate.notNull(oauthConfig.get(SnowflakeConstants.REDIRECT_URI), "Missing required property: redirect_uri");
            
            LOGGER.debug("First time auth. Using authorization_code...");
            JSONObject tokenJson = getTokenFromAuthCode(authCode, redirectUri, tokenEndpoint, clientId, clientSecret);
            saveTokenToSecretsManager(tokenJson, oauthConfig);
            return tokenJson.getString(ACCESS_TOKEN);
        } 
        else {
            // Refresh existing token
            String refreshToken = oauthConfig.get(REFRESH_TOKEN);
            if (refreshToken == null) {
                throw new RuntimeException("No refresh token available and access token is expired");
            }
            
            LOGGER.debug("Access token expired. Using refresh_token...");
            JSONObject refreshed = refreshAccessToken(refreshToken, tokenEndpoint, clientId, clientSecret);
            refreshed.put(REFRESH_TOKEN, refreshToken); // Keep the same refresh token
            saveTokenToSecretsManager(refreshed, oauthConfig);
            return refreshed.getString(ACCESS_TOKEN);
        }
    }
    
    private void saveTokenToSecretsManager(JSONObject tokenJson, Map<String, String> oauthConfig)
    {
        // Update token related fields
        tokenJson.put(FETCHED_AT, System.currentTimeMillis() / 1000);
        oauthConfig.put(ACCESS_TOKEN, tokenJson.getString(ACCESS_TOKEN));
        if (tokenJson.has(REFRESH_TOKEN)) {
            oauthConfig.put(REFRESH_TOKEN, tokenJson.getString(REFRESH_TOKEN));
        }
        oauthConfig.put(EXPIRES_IN, String.valueOf(tokenJson.getInt(EXPIRES_IN)));
        oauthConfig.put(FETCHED_AT, String.valueOf(tokenJson.getLong(FETCHED_AT)));
        
        // Save updated secret
        secretsClient.putSecretValue(builder -> builder
                .secretId(this.oauthSecretName)
                .secretString(String.valueOf(new JSONObject(oauthConfig)))
                .build());
    }
    
    private JSONObject getTokenFromAuthCode(String authCode, String redirectUri, String tokenEndpoint, String clientId, String clientSecret) throws Exception
    {
        String body = "grant_type=authorization_code"
                + "&code=" + authCode
                + "&redirect_uri=" + redirectUri;
        
        return requestToken(body, tokenEndpoint, clientId, clientSecret);
    }
    
    private JSONObject refreshAccessToken(String refreshToken, String tokenEndpoint, String clientId, String clientSecret) throws Exception
    {
        String body = "grant_type=refresh_token"
                + "&refresh_token=" + URLEncoder.encode(refreshToken, StandardCharsets.UTF_8);
        
        return requestToken(body, tokenEndpoint, clientId, clientSecret);
    }
    
    private JSONObject requestToken(String requestBody, String tokenEndpoint, String clientId, String clientSecret) throws Exception
    {
        HttpURLConnection conn = getHttpURLConnection(tokenEndpoint, clientId, clientSecret);
        
        try (OutputStream os = conn.getOutputStream()) {
            os.write(requestBody.getBytes(StandardCharsets.UTF_8));
        }
        
        int responseCode = conn.getResponseCode();
        InputStream is = (responseCode >= 200 && responseCode < 300) ?
                conn.getInputStream() : conn.getErrorStream();
        
        String response = new BufferedReader(new InputStreamReader(is))
                .lines()
                .reduce("", (acc, line) -> acc + line);
        
        if (responseCode != 200) {
            throw new RuntimeException("Failed to refresh token: " + responseCode + " - " + response);
        }
        
        JSONObject tokenJson = new JSONObject(response);
        tokenJson.put(FETCHED_AT, System.currentTimeMillis() / 1000);
        return tokenJson;
    }
    
    static HttpURLConnection getHttpURLConnection(String tokenEndpoint, String clientId, String clientSecret) throws IOException
    {
        URL url = new URL(tokenEndpoint);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        
        String authHeader = Base64.getEncoder()
                .encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));
        
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Basic " + authHeader);
        conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        conn.setDoOutput(true);
        return conn;
    }
} 