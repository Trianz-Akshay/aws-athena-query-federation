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

import com.amazonaws.athena.connector.credentials.DefaultCredentials;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SqlServerCredentialsProviderTest
{
    private static final String TEST_SECRET_NAME = "test-secret";
    private static final String TEST_CLIENT_ID = "test-client-id";
    private static final String TEST_CLIENT_SECRET = "test-client-secret";
    private static final String TEST_TENANT_ID = "test-tenant-id";
    private static final String TEST_USERNAME = "test-username";
    private static final String TEST_PASSWORD = "test-password";
    private static final String TEST_ACCESS_TOKEN = "test-access-token";

    @Mock
    private SecretsManagerClient mockSecretsClient;

    @Before
    public void setUp()
    {
        // Setup mock behavior
    }

    @Test
    public void testGetCredentialWithOAuthFlow() throws Exception
    {
        String secretJson = createOAuthSecretJson();

        try (MockedConstruction<CachableSecretsManager> mockedConstruction = mockConstruction(CachableSecretsManager.class,
                (mock, context) -> {
                    when(mock.getSecret(TEST_SECRET_NAME)).thenReturn(secretJson);
                    when(mock.getSecretsManager()).thenReturn(mockSecretsClient);
                })) {

            SqlServerCredentialsProvider provider = new SqlServerCredentialsProvider(TEST_SECRET_NAME, mockSecretsClient);

            try (MockedStatic<SqlServerCredentialsProvider> mockedStatic = Mockito.mockStatic(SqlServerCredentialsProvider.class)) {
                HttpURLConnection mockConnection = createMockHttpConnection(200, createTokenResponse());
                mockedStatic.when(() -> SqlServerCredentialsProvider.getHttpURLConnection(anyString()))
                        .thenReturn(mockConnection);

                DefaultCredentials credentials = provider.getCredential();

                assertNotNull(credentials);
                assertEquals(TEST_USERNAME, credentials.getUser());
                assertEquals(TEST_ACCESS_TOKEN, credentials.getPassword());
            }
        }
    }

    @Test
    public void testGetCredentialMapWithOAuthFlow() throws Exception
    {
        String secretJson = createOAuthSecretJson();

        try (MockedConstruction<CachableSecretsManager> mockedConstruction = mockConstruction(CachableSecretsManager.class,
                (mock, context) -> {
                    when(mock.getSecret(TEST_SECRET_NAME)).thenReturn(secretJson);
                    when(mock.getSecretsManager()).thenReturn(mockSecretsClient);
                })) {

            SqlServerCredentialsProvider provider = new SqlServerCredentialsProvider(TEST_SECRET_NAME, mockSecretsClient);

            try (MockedStatic<SqlServerCredentialsProvider> mockedStatic = Mockito.mockStatic(SqlServerCredentialsProvider.class)) {
                HttpURLConnection mockConnection = createMockHttpConnection(200, createTokenResponse());
                mockedStatic.when(() -> SqlServerCredentialsProvider.getHttpURLConnection(anyString()))
                        .thenReturn(mockConnection);

                Map<String, String> credentialMap = provider.getCredentialMap();

                assertNotNull(credentialMap);
                assertEquals(TEST_USERNAME, credentialMap.get("user"));
                assertEquals(TEST_ACCESS_TOKEN, credentialMap.get("password"));
                assertEquals("ActiveDirectoryServicePrincipal", credentialMap.get("authentication"));
            }
        }
    }

    @Test
    public void testGetCredentialWithStandardCredentials() throws Exception
    {
        String secretJson = createStandardSecretJson();

        try (MockedConstruction<CachableSecretsManager> mockedConstruction = mockConstruction(CachableSecretsManager.class,
                (mock, context) -> {
                    when(mock.getSecret(TEST_SECRET_NAME)).thenReturn(secretJson);
                    when(mock.getSecretsManager()).thenReturn(mockSecretsClient);
                })) {

            SqlServerCredentialsProvider provider = new SqlServerCredentialsProvider(TEST_SECRET_NAME, mockSecretsClient);

            DefaultCredentials credentials = provider.getCredential();

            assertNotNull(credentials);
            assertEquals(TEST_USERNAME, credentials.getUser());
            assertEquals(TEST_PASSWORD, credentials.getPassword());
        }
    }

    @Test
    public void testGetCredentialMapWithStandardCredentials() throws Exception
    {
        String secretJson = createStandardSecretJson();

        try (MockedConstruction<CachableSecretsManager> mockedConstruction = mockConstruction(CachableSecretsManager.class,
                (mock, context) -> {
                    when(mock.getSecret(TEST_SECRET_NAME)).thenReturn(secretJson);
                    when(mock.getSecretsManager()).thenReturn(mockSecretsClient);
                })) {

            SqlServerCredentialsProvider provider = new SqlServerCredentialsProvider(TEST_SECRET_NAME, mockSecretsClient);

            Map<String, String> credentialMap = provider.getCredentialMap();

            assertNotNull(credentialMap);
            assertEquals(TEST_USERNAME, credentialMap.get("user"));
            assertEquals(TEST_PASSWORD, credentialMap.get("password"));
        }
    }

    @Test(expected = RuntimeException.class)
    public void testGetCredentialWithInvalidSecret() throws Exception
    {
        try (MockedConstruction<CachableSecretsManager> mockedConstruction = mockConstruction(CachableSecretsManager.class,
                (mock, context) -> {
                    when(mock.getSecret(TEST_SECRET_NAME)).thenReturn("invalid-json");
                    when(mock.getSecretsManager()).thenReturn(mockSecretsClient);
                })) {

            SqlServerCredentialsProvider provider = new SqlServerCredentialsProvider(TEST_SECRET_NAME, mockSecretsClient);
            provider.getCredential();
        }
    }

    @Test(expected = RuntimeException.class)
    public void testGetCredentialWithMissingRequiredFields() throws Exception
    {
        String secretJson = createIncompleteOAuthSecretJson();

        try (MockedConstruction<CachableSecretsManager> mockedConstruction = mockConstruction(CachableSecretsManager.class,
                (mock, context) -> {
                    when(mock.getSecret(TEST_SECRET_NAME)).thenReturn(secretJson);
                    when(mock.getSecretsManager()).thenReturn(mockSecretsClient);
                })) {

            SqlServerCredentialsProvider provider = new SqlServerCredentialsProvider(TEST_SECRET_NAME, mockSecretsClient);
            provider.getCredential();
        }
    }

    private String createOAuthSecretJson()
    {
        JSONObject json = new JSONObject();
        json.put("client_id", TEST_CLIENT_ID);
        json.put("client_secret", TEST_CLIENT_SECRET);
        json.put("tenant_id", TEST_TENANT_ID);
        json.put("username", TEST_USERNAME);
        return json.toString();
    }

    private String createStandardSecretJson()
    {
        JSONObject json = new JSONObject();
        json.put("username", TEST_USERNAME);
        json.put("password", TEST_PASSWORD);
        return json.toString();
    }

    private String createIncompleteOAuthSecretJson()
    {
        JSONObject json = new JSONObject();
        json.put("client_id", TEST_CLIENT_ID);
        // Missing client_secret and tenant_id
        json.put("username", TEST_USERNAME);
        return json.toString();
    }

    private String createTokenResponse()
    {
        JSONObject json = new JSONObject();
        json.put("access_token", TEST_ACCESS_TOKEN);
        json.put("expires_in", 3600);
        json.put("token_type", "Bearer");
        return json.toString();
    }

    private HttpURLConnection createMockHttpConnection(int responseCode, String responseBody) throws IOException
    {
        HttpURLConnection mockConnection = Mockito.mock(HttpURLConnection.class);
        when(mockConnection.getResponseCode()).thenReturn(responseCode);
        when(mockConnection.getOutputStream()).thenReturn(Mockito.mock(OutputStream.class));
        
        ByteArrayInputStream inputStream = new ByteArrayInputStream(responseBody.getBytes());
        when(mockConnection.getInputStream()).thenReturn(inputStream);
        
        return mockConnection;
    }

    private static <T> MockedConstruction<T> mockConstruction(Class<T> clazz, MockedConstruction.MockInitializer<T> mockInitializer)
    {
        return Mockito.mockConstruction(clazz, mockInitializer);
    }
} 