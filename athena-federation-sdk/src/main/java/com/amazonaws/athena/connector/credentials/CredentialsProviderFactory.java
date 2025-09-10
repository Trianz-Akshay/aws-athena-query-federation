/*-
 * #%L
 * athena-federation-sdk
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
package com.amazonaws.athena.connector.credentials;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.security.CachableSecretsManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.io.IOException;
import java.util.Map;

/**
 * Factory class for handling credentials provider creation with dependency injection support.
 * This class can be used by any connector that needs to support OAuth authentication
 * with fallback to default username/password credentials.
 * 
 * Usage examples:
 * <pre>
 * // Basic usage with OAuth provider
 * factory = new CredentialsProviderFactory(secret, secretManager, oauthProvider);
 * 
 * // Only default credentials (no OAuth)
 * factory = new CredentialsProviderFactory(secret, secretManager, null);
 * </pre>
 */
public final class CredentialsProviderFactory
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    private final String secretName;
    private final CachableSecretsManager secretsManager;
    private final OAuthCredentialsProvider oauthProvider;

    /**
     * Constructor with a single OAuth credential provider.
     *
     * @param secretName The name of the secret in AWS Secrets Manager
     * @param secretsManager The secrets manager instance
     * @param oauthProvider The OAuth credential provider (can be null)
     */
    public CredentialsProviderFactory(String secretName, CachableSecretsManager secretsManager, 
                                     OAuthCredentialsProvider oauthProvider)
    {
        this.secretName = secretName;
        this.secretsManager = secretsManager;
        this.oauthProvider = oauthProvider;
    }

    /**
     * Creates a credentials provider based on the injected OAuth provider and secret configuration.
     * If OAuth is configured, initializes and returns the OAuth provider. Otherwise, falls back to 
     * default username/password credentials.
     *
     * @return A new CredentialsProvider instance based on the secret configuration
     * @throws AthenaConnectorException if there are errors deserializing the secret or creating the provider
     */
    public CredentialsProvider createCredentialProvider()
    {
        if (StringUtils.isNotBlank(secretName)) {
            try {
                String secretString = secretsManager.getSecret(secretName);
                @SuppressWarnings("unchecked")
                Map<String, String> secretMap = OBJECT_MAPPER.readValue(secretString, Map.class);

                // Try OAuth provider if available
                if (oauthProvider != null) {
                    try {
                        // Check if OAuth is configured for this provider
                        if (oauthProvider.isOAuthConfigured(secretMap)) {
                            oauthProvider.initialize(secretName, secretMap, secretsManager);
                            return oauthProvider;
                        }
                    }
                    catch (RuntimeException e) {
                        // Log the error but continue to fallback
                        // This allows for graceful fallback to default credentials
                    }
                }
                
                // Fall back to default credentials if OAuth is not configured or provider is null
                return new DefaultCredentialsProvider(secretString);
            }
            catch (IOException ioException) {
                throw new AthenaConnectorException("Could not deserialize credentials into HashMap: ",
                        ErrorDetails.builder()
                                .errorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION.toString())
                                .errorMessage(ioException.getMessage())
                                .build());
            }
        }

        return null;
    }

    /**
     * Static method for backward compatibility with existing code.
     * Creates a credentials provider using the old static factory pattern.
     * 
     * @param secretName The name of the secret in AWS Secrets Manager
     * @param secretsManager The secrets manager instance
     * @param oauthProvider The OAuth credential provider (can be null)
     * @return A new CredentialsProvider instance based on the secret configuration
     * @throws AthenaConnectorException if there are errors deserializing the secret or creating the provider
     * @deprecated Use the new dependency injection approach: new CredentialsProviderFactory(...).createCredentialProvider()
     */
    @Deprecated
    public static CredentialsProvider createCredentialProvider(
            String secretName,
            CachableSecretsManager secretsManager,
            OAuthCredentialsProvider oauthProvider)
    {
        CredentialsProviderFactory factory = new CredentialsProviderFactory(secretName, secretsManager, oauthProvider);
        return factory.createCredentialProvider();
    }
}
