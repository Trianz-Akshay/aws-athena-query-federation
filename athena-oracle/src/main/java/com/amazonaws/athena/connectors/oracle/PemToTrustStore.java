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
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class PemToTrustStore
{
    private PemToTrustStore()
    {
    }
    private static final Logger logger = LoggerFactory.getLogger(PemToTrustStore.class);
    private static final String S3_BUCKET_NAME = "athena-vertica-data-spill";
    private static final String S3_OBJECT_KEY = "rds-global/oracle-server.crt";
    private static final String LOCAL_PEM_PATH = "/tmp/oracle-server.crt";
    private static final String TRUSTSTORE_PATH = System.getProperty("java.home") + "/lib/security/cacerts";
    private static final String TRUSTSTORE_PASSWORD = "changeit";
    private static final String CERT_ALIAS = "my-cert-alias";

    public static void main()
    {
        try {
            // Step 1: Download the PEM file from S3
            downloadPemFromS3();

            // Step 2: Import the PEM file into the cacerts truststore
            importPemToTrustStore();

        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void downloadPemFromS3() throws IOException
    {
        logger.error("Downloading PEM file from S3...");

        // Create an S3 client using AWS SDK v2
        S3Client s3Client = S3Client.builder()
                .region(Region.US_EAST_1) // Change region as needed
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        // Create a GetObjectRequest
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(S3_BUCKET_NAME)
                .key(S3_OBJECT_KEY)
                .build();

        // Download the PEM file from S3
        try (InputStream s3InputStream = s3Client.getObject(getObjectRequest);
             FileOutputStream outputStream = new FileOutputStream(LOCAL_PEM_PATH)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = s3InputStream.read(buffer)) > 0) {
                outputStream.write(buffer, 0, bytesRead);
            }
        }
        logger.error("PEM file downloaded to: " + LOCAL_PEM_PATH);
    }

    private static void importPemToTrustStore() throws IOException, InterruptedException
    {
        logger.error("Importing PEM file into truststore...");
//        ProcessBuilder processBuilder1 = new ProcessBuilder(
//                "chmod",
//                "755",
//                "/var/lang/lib/security/cacerts"
//        );
//        Process process1 = processBuilder1.start();
//        int exitCode1 = process1.waitFor();
//        try (InputStream inputStream1 = process1.getInputStream()) {
//            String output = new String(inputStream1.readAllBytes());
//            System.out.println(" Output: " + output);
//        }
//
//        if (exitCode1 != 0) {
//            System.err.println("Failed " + exitCode1);
//        }
//        else {
//            logger.error("successfully.");
//        }
        ProcessBuilder processBuilder = new ProcessBuilder(
                "keytool",
                "-importcert",
               // "-cacerts",
                "-file", LOCAL_PEM_PATH,
                "-keystore", "/usr/lib/jvm/java-11-openjdk/lib/security/cacerts",
                "-storepass", TRUSTSTORE_PASSWORD,
                "-noprompt",
                "-alias", CERT_ALIAS
        );

        Process process = processBuilder.start();
        try (InputStream inputStream = process.getInputStream()) {
            String output = new String(inputStream.readAllBytes());
            System.out.println("Keytool Output: " + output);
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            System.err.println("Failed to import certificate. Exit Code: " + exitCode);
        }
        else {
            logger.error("Certificate imported successfully.");
        }
    }
}
