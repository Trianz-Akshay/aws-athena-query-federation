/*-
 * #%L
 * athena-oracle
 * %%
 * Copyright (C) 2019 - 2024 Amazon Web Services
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
package com.amazonaws.athena.connectors.oracle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;

public class OracleUtils
{
    public static final String TMP_CACERTS = "/tmp/cacerts";
    private static final Logger logger = LoggerFactory.getLogger(OracleUtils.class);
    private static final String LOCAL_PEM_PATH = "/tmp/oracle-server.crt";
    private static final String TRUSTSTORE_PATH = System.getProperty("java.home") + "/lib/security/cacerts";
    private static final String TRUSTSTORE_PASSWORD = "changeit";
    private static final String CERT_ALIAS = "rds-global";

    private OracleUtils()
    {
    }

    public static void installCaCertificate(Map<String, String> environment)
    {
        try {
            Path source = Paths.get(TRUSTSTORE_PATH);
            Path target = Paths.get(TMP_CACERTS);

            // Copy the file (overwrite if the file already exists)
            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
            // Step 1: Download the PEM file from S3
            downloadPemFromS3(environment.get("db_server_cert_S3Path"));

            // Step 2: Import the PEM file into the cacerts truststore
            importPemToTrustStore();

        }
        catch (Exception e) {
            logger.error("Oracle db server cert import failed with reason : {}", e.getMessage());
        }
    }

    private static void downloadPemFromS3(String s3Path) throws IOException
    {
        String bucketName = s3Path.split("://")[1].split("/")[0];
        String objectKey = s3Path.substring(s3Path.indexOf("/", 5) + 1);

        // Download the PEM file from S3
        try (InputStream s3InputStream = S3Client.create().getObject(GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build());
             FileOutputStream outputStream = new FileOutputStream(LOCAL_PEM_PATH)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = s3InputStream.read(buffer)) > 0) {
                outputStream.write(buffer, 0, bytesRead);
            }
        }
        logger.debug("PEM file downloaded to: " + LOCAL_PEM_PATH);
    }

    private static void importPemToTrustStore() throws IOException, InterruptedException
    {
        ProcessBuilder processBuilder = new ProcessBuilder(
                "keytool",
                "-importcert",
                "-file", LOCAL_PEM_PATH,
                "-keystore", TMP_CACERTS,
                "-storepass", TRUSTSTORE_PASSWORD,
                "-noprompt",
                "-alias", CERT_ALIAS
        );

        Process process = processBuilder.start();
        try (InputStream inputStream = process.getInputStream()) {
            String output = new String(inputStream.readAllBytes());
            logger.debug("Keytool Output: {}", output);
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            logger.error("Failed to import certificate. Exit Code: {}", exitCode);
        }
        else {
            logger.info("Certificate imported successfully.");
        }
        System.setProperty("javax.net.ssl.trustStore", TMP_CACERTS);
        System.setProperty("javax.net.ssl.trustStorePassword", TRUSTSTORE_PASSWORD);
    }
}
