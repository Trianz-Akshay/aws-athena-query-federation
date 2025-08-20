/*-
 * #%L
 * Amazon Athena JDBC Connector
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
package com.amazonaws.athena.connectors.jdbc.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupFile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import static java.util.Objects.requireNonNull;

/**
 * Base factory for creating query builders with StringTemplate support.
 * Enhanced with sophisticated string template patterns and logging similar to getPartitionWhereClauses.
 * Provides common functionality for loading and managing StringTemplate files.
 * Includes quote character management functionality from CommonQueryFactory.
 */
public abstract class BaseQueryFactory
{
    protected static final Logger logger = LoggerFactory.getLogger(BaseQueryFactory.class);
    protected static final String TEST_TEMPLATE = "test_template";
    
    private final String templateFile;
    private final String localTemplateFile;
    private volatile boolean useLocalFallback = false;
    private final String quoteChar;

    protected BaseQueryFactory(String templateFile, String quoteChar)
    {
        this.templateFile = templateFile;
        this.localTemplateFile = "/tmp/" + templateFile;
        this.quoteChar = quoteChar;
        logger.debug("BaseQueryFactory initialized with templateFile: {} and quoteChar: {}", templateFile, quoteChar);
    }

    /**
     * Enhanced STGroupFile creation with comprehensive logging and error handling.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    protected STGroupFile createGroupFile()
    {
        logger.debug("createGroupFile - Creating STGroupFile from: {}", templateFile);
        
        if (!useLocalFallback) {
            try {
                STGroupFile stGroupFile = new STGroupFile(templateFile);
                requireNonNull(stGroupFile.getInstanceOf(TEST_TEMPLATE), "Test template must not be null");
                logger.debug("createGroupFile - Successfully created STGroupFile from: {}", templateFile);
                return stGroupFile;
            }
            catch (RuntimeException ex) {
                logger.info("createGroupFile: Error while attempting to load STGroupFile from: {}. Falling back to local file.", templateFile, ex);
                return createLocalGroupFile();
            }
        }

        logger.debug("createGroupFile - Using local fallback file: {}", localTemplateFile);
        STGroupFile stGroupFile = new STGroupFile(localTemplateFile);
        requireNonNull(stGroupFile.getInstanceOf(TEST_TEMPLATE), "Test template must not be null");
        return stGroupFile;
    }

    /**
     * Enhanced local group file creation with comprehensive logging and error handling.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    private STGroupFile createLocalGroupFile()
    {
        logger.info("createLocalGroupFile: Attempting STGroupFile fallback to: {}", localTemplateFile);
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(templateFile);
        if (in == null) {
            logger.error("createLocalGroupFile: Could not find template file in classpath: {}", templateFile);
            throw new RuntimeException("Template file not found: " + templateFile);
        }
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        StringBuilder sb = new StringBuilder();
        try {
            String line = reader.readLine();
            sb.append(line);
            while (line != null) {
                line = reader.readLine();
                if (line != null) {
                    sb.append(line);
                }
            }

            BufferedWriter writer = new BufferedWriter(new FileWriter(localTemplateFile));
            writer.write(sb.toString());
            writer.close();
            logger.debug("createLocalGroupFile: Successfully wrote template to local file: {}", localTemplateFile);
        }
        catch (IOException ex) {
            logger.error("createLocalGroupFile: Exception while creating local template file: {}", localTemplateFile, ex);
            throw new RuntimeException("Failed to create local template file", ex);
        }

        useLocalFallback = true;
        logger.info("createLocalGroupFile: Template content length: {} characters", sb.length());

        STGroupFile stGroupFile = new STGroupFile(localTemplateFile);
        requireNonNull(stGroupFile.getInstanceOf(TEST_TEMPLATE), "Test template must not be null");
        logger.debug("createLocalGroupFile: Successfully created STGroupFile from local file");
        return stGroupFile;
    }

    /**
     * Enhanced query template retrieval with logging and validation.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    public ST getQueryTemplate(String templateName)
    {
        logger.debug("getQueryTemplate - Retrieving template: {} from file: {}", templateName, templateFile);
        ST template = createGroupFile().getInstanceOf(templateName);
        if (template == null) {
            logger.error("getQueryTemplate: Template not found: {} in file: {}", templateName, templateFile);
            throw new RuntimeException("Template not found: " + templateName);
        }
        logger.debug("getQueryTemplate - Successfully retrieved template: {}", templateName);
        return template;
    }

    /**
     * Create a query builder instance. Must be implemented by subclasses.
     * Enhanced with logging similar to getPartitionWhereClauses pattern.
     */
    public abstract BaseQueryBuilder createQueryBuilder();

    /**
     * Enhanced quote character retrieval with logging.
     * Common implementation for database-specific query factories.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    protected String getQuoteChar()
    {
        logger.debug("getQuoteChar - Returning quote character: {}", quoteChar);
        return quoteChar;
    }

    /**
     * Get the quote character for this database type.
     * Useful for subclasses that need access to the quote character.
     *
     * @return The quote character
     */
    protected String getDatabaseQuoteChar()
    {
        return quoteChar;
    }
} 
