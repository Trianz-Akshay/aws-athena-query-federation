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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;

import java.util.List;

/**
 * Enhanced base query builder that provides common functionality for database-specific query builders.
 * This class extracts common patterns found in MySQL and SQL Server query builders to reduce code duplication.
 * Enhanced with sophisticated string template patterns and logging similar to getPartitionWhereClauses.
 */
public abstract class EnhancedBaseQueryBuilder extends BaseQueryBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(EnhancedBaseQueryBuilder.class);
    
    protected Split currentSplit;

    protected EnhancedBaseQueryBuilder(ST template, String quoteChar)
    {
        super(template, quoteChar);
        logger.debug("EnhancedBaseQueryBuilder initialized with quoteChar: {}", quoteChar);
    }

    /**
     * Enhanced split handling with logging and validation.
     * Common implementation for database-specific query builders.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     */
    @Override
    public EnhancedBaseQueryBuilder withSplit(Split split)
    {
        logger.debug("withSplit - Setting split with {} properties", split.getProperties().size());
        this.currentSplit = split;
        return this;
    }

    /**
     * Enhanced conjuncts building with common partition support logic.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     * This method provides the common structure while delegating specific implementations to subclasses.
     */
    @Override
    public EnhancedBaseQueryBuilder withConjuncts(org.apache.arrow.vector.types.pojo.Schema schema, Constraints constraints)
    {
        logger.debug("withConjuncts - Building conjuncts for schema with {} fields", schema.getFields().size());
        this.conjuncts = buildConjuncts(schema.getFields(), constraints, this.parameterValues);

        // Add database-specific partition clauses if split is available
        if (currentSplit != null) {
            List<String> partitionClauses = buildPartitionWhereClauses(currentSplit);
            if (!partitionClauses.isEmpty()) {
                logger.debug("withConjuncts - Adding {} partition clauses", partitionClauses.size());
                this.conjuncts.addAll(partitionClauses);
            }
        }

        logger.debug("withConjuncts - Generated {} conjuncts with {} parameter values",
            this.conjuncts.size(), this.parameterValues.size());
        return this;
    }

    /**
     * Build database-specific partition WHERE clauses.
     * Must be implemented by subclasses to provide database-specific partition handling.
     * Similar to the pattern used in getPartitionWhereClauses for consistent SQL generation.
     *
     * @param split The split containing partition information
     * @return List of partition WHERE clauses
     */
    protected abstract List<String> buildPartitionWhereClauses(Split split);

    /**
     * Get the current split being processed.
     * Useful for subclasses that need access to split information.
     *
     * @return The current split or null if not set
     */
    protected Split getCurrentSplit()
    {
        return currentSplit;
    }
} 
