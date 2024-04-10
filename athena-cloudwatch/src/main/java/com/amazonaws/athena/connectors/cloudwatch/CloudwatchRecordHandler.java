/*-
 * #%L
 * athena-cloudwatch
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.cloudwatch;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.Range;
import com.amazonaws.athena.connector.lambda.domain.predicate.SortedRangeSet;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.cloudwatch.qpt.CloudwatchQueryPassthrough;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClientBuilder;
import com.amazonaws.services.logs.model.GetLogEventsRequest;
import com.amazonaws.services.logs.model.GetLogEventsResult;
import com.amazonaws.services.logs.model.GetQueryResultsResult;
import com.amazonaws.services.logs.model.OutputLogEvent;
import com.amazonaws.services.logs.model.ResultField;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static com.amazonaws.athena.connectors.cloudwatch.CloudwatchExceptionFilter.EXCEPTION_FILTER;
import static com.amazonaws.athena.connectors.cloudwatch.CloudwatchMetadataHandler.LOG_GROUP_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.CloudwatchMetadataHandler.LOG_MSG_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.CloudwatchMetadataHandler.LOG_STREAM_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.CloudwatchMetadataHandler.LOG_TIME_FIELD;
import static com.amazonaws.athena.connectors.cloudwatch.CloudwatchUtils.getResult;

/**
 * Handles data read record requests for the Athena Cloudwatch Connector.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Reads and maps Cloudwatch Logs data for a specific LogStream (split)
 * 2. Attempts to push down time range predicates into Cloudwatch.
 */
public class CloudwatchRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(CloudwatchRecordHandler.class);
    //Used to tag log lines generated by this connector for diagnostic purposes when interacting with Athena.
    private static final String SOURCE_TYPE = "cloudwatch";
    //Used to handle Throttling events and apply AIMD congestion control
    private final ThrottlingInvoker invoker;
    private final AtomicLong count = new AtomicLong(0);
    private final AWSLogs awsLogs;
    private final CloudwatchQueryPassthrough queryPassthrough = new CloudwatchQueryPassthrough();

    public CloudwatchRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(
                AmazonS3ClientBuilder.defaultClient(),
                AWSSecretsManagerClientBuilder.defaultClient(),
                AmazonAthenaClientBuilder.defaultClient(),
                AWSLogsClientBuilder.defaultClient(),
                configOptions);
    }

    @VisibleForTesting
    protected CloudwatchRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena athena, AWSLogs awsLogs, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, SOURCE_TYPE, configOptions);
        this.awsLogs = awsLogs;
        this.invoker = ThrottlingInvoker.newDefaultBuilder(EXCEPTION_FILTER, configOptions).build();
    }

    /**
     * Scans Cloudwatch Logs using the LogStream and optional Time stamp filters.
     *
     * @see RecordHandler
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
            throws TimeoutException, InterruptedException
    {
        if (recordsRequest.getConstraints().isQueryPassThrough()) {
            Map<String, String> qptArguments = recordsRequest.getConstraints().getQueryPassthroughArguments();
            queryPassthrough.verify(qptArguments);
            GetQueryResultsResult getQueryResultsResult = getResult(invoker, awsLogs, qptArguments, Integer.parseInt(qptArguments.get(CloudwatchQueryPassthrough.LIMIT)));

            for (List<ResultField> resultList : getQueryResultsResult.getResults()) {
                spiller.writeRows((Block block, int rowNum) -> {
                    for (ResultField resultField : resultList) {
                        boolean matched = true;
                        logger.info("field {} , value {}", resultField.getField(), resultField.getValue());
                        matched &= block.offerValue(resultField.getField(), rowNum, resultField.getValue());
                        if (!matched) {
                            return 0;
                        }
                    }
                    return 1;
                });
            }
        }
        else {
            String continuationToken = null;
            TableName tableName = recordsRequest.getTableName();
            Split split = recordsRequest.getSplit();
            invoker.setBlockSpiller(spiller);
            do {
                final String actualContinuationToken = continuationToken;
                GetLogEventsResult logEventsResult = invoker.invoke(() -> awsLogs.getLogEvents(
                        pushDownConstraints(recordsRequest.getConstraints(),
                                new GetLogEventsRequest()
                                        .withLogGroupName(split.getProperty(LOG_GROUP_FIELD))
                                        //We use the property instead of the table name because of the special all_streams table
                                        .withLogStreamName(split.getProperty(LOG_STREAM_FIELD))
                                        .withNextToken(actualContinuationToken)
                                        // must be set to use nextToken correctly
                                        .withStartFromHead(true)
                        )));

                if (continuationToken == null || !continuationToken.equals(logEventsResult.getNextForwardToken())) {
                    continuationToken = logEventsResult.getNextForwardToken();
                }
                else {
                    continuationToken = null;
                }

                for (OutputLogEvent ole : logEventsResult.getEvents()) {
                    spiller.writeRows((Block block, int rowNum) -> {
                        boolean matched = true;
                        matched &= block.offerValue(LOG_STREAM_FIELD, rowNum, split.getProperty(LOG_STREAM_FIELD));
                        matched &= block.offerValue(LOG_TIME_FIELD, rowNum, ole.getTimestamp());
                        matched &= block.offerValue(LOG_MSG_FIELD, rowNum, ole.getMessage());
                        return matched ? 1 : 0;
                    });
                }

                logger.info("readWithConstraint: LogGroup[{}] LogStream[{}] Continuation[{}] rows[{}]",
                        tableName.getSchemaName(), tableName.getTableName(), continuationToken,
                        logEventsResult.getEvents().size());
            }
            while (continuationToken != null && queryStatusChecker.isQueryRunning());
        }
    }

    /**
     * Attempts to push down predicates into Cloudwatch Logs by decorating the Cloudwatch Logs request.
     *
     * @param constraints The constraints for the read as provided by Athena based on the customer's query.
     * @param request     The Cloudwatch Logs request to inject predicates to.
     * @return The decorated Cloudwatch Logs request.
     * @note This impl currently only pushing down SortedRangeSet filters (>=, =<, between) on the log time column.
     */
    private GetLogEventsRequest pushDownConstraints(Constraints constraints, GetLogEventsRequest request)
    {
        ValueSet timeConstraint = constraints.getSummary().get(LOG_TIME_FIELD);
        if (timeConstraint instanceof SortedRangeSet && !timeConstraint.isNullAllowed()) {
            //SortedRangeSet is how >, <, between is represented which are easiest and most common when
            //searching logs so we attempt to push that down here as an optimization. SQL can represent complex
            //overlapping ranges which Cloudwatch can not support so this is not a replacement for applying
            //constraints using the ConstraintEvaluator.

            Range basicPredicate = ((SortedRangeSet) timeConstraint).getSpan();

            if (!basicPredicate.getLow().isNullValue()) {
                Long lowerBound = (Long) basicPredicate.getLow().getValue();
                request.setStartTime(lowerBound);
            }

            if (!basicPredicate.getHigh().isNullValue()) {
                Long upperBound = (Long) basicPredicate.getHigh().getValue();
                request.setEndTime(upperBound);
            }
        }

        return request;
    }
}
