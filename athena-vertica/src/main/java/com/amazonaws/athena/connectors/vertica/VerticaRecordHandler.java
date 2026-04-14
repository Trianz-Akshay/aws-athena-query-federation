/*-
 * #%L
 * athena-vertica
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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

package com.amazonaws.athena.connectors.vertica;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.writers.GeneratedRowWriter;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BigIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.BitExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateDayExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DateMilliExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.DecimalExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float4Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.Float8Extractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.SmallIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.TinyIntExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarBinaryExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.extractors.VarCharExtractor;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableDecimalHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarBinaryHolder;
import com.amazonaws.athena.connector.lambda.data.writers.holders.NullableVarCharHolder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static com.amazonaws.athena.connectors.vertica.VerticaConstants.VERTICA_SPLIT_EXPORT_BUCKET;
import static com.amazonaws.athena.connectors.vertica.VerticaConstants.VERTICA_SPLIT_OBJECT_KEY;
import static com.amazonaws.athena.connectors.vertica.VerticaConstants.VERTICA_SPLIT_QUERY_ID;

public class VerticaRecordHandler
        extends RecordHandler
{
    private static final Logger logger = LoggerFactory.getLogger(VerticaRecordHandler.class);
    private static final String SOURCE_TYPE = "vertica";

    public VerticaRecordHandler(java.util.Map<String, String> configOptions)
    {
        this(S3Client.create(),
                SecretsManagerClient.create(),
                AthenaClient.create(), configOptions);
    }

    @VisibleForTesting
    protected VerticaRecordHandler(S3Client amazonS3, SecretsManagerClient secretsManager, AthenaClient amazonAthena, java.util.Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, amazonAthena, SOURCE_TYPE, configOptions);
    }

    /**
     * Used to read the row data associated with the provided Split.
     *
     * @param spiller            A BlockSpiller that should be used to write the row data associated with this Split.
     *                           The BlockSpiller automatically handles chunking the response, encrypting, and spilling to S3.
     * @param recordsRequest     Details of the read request, including:
     *                           1. The Split
     *                           2. The Catalog, Database, and Table the read request is for.
     *                           3. The filtering predicate (if any)
     *                           4. The columns required for projection.
     * @param queryStatusChecker A QueryStatusChecker that you can use to stop doing work for a query that has already terminated
     * @throws IOException Throws an IOException
     */
    @Override
    protected void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker)
            throws IOException
    {
        logger.info("readWithConstraint: schema[{}] tableName[{}]", recordsRequest.getSchema(), recordsRequest.getTableName());

        Schema schemaName = recordsRequest.getSchema();
        Split split = recordsRequest.getSplit();
        String id = split.getProperty(VERTICA_SPLIT_QUERY_ID);
        String exportBucket = split.getProperty(VERTICA_SPLIT_EXPORT_BUCKET);
        String s3ObjectKey = split.getProperty(VERTICA_SPLIT_OBJECT_KEY);

        if (!s3ObjectKey.isEmpty()) {
            //get column name and type from the Schema
            HashMap<String, Types.MinorType> mapOfNamesAndTypes = new HashMap<>();
            HashMap<String, Object> mapOfCols = new HashMap<>();

            for (Field field : schemaName.getFields()) {
                Types.MinorType minorTypeForArrowType = Types.getMinorTypeForArrowType(field.getType());
                mapOfNamesAndTypes.put(field.getName(), minorTypeForArrowType);
                mapOfCols.put(field.getName(), null);
            }


            // creating a RowContext class to hold the column name and value.
            final RowContext rowContext = new RowContext(id);

            //Generating the RowWriter and Extractor
            GeneratedRowWriter.RowWriterBuilder builder = GeneratedRowWriter.newBuilder(recordsRequest.getConstraints());
            for (Field next : recordsRequest.getSchema().getFields()) {
                Extractor extractor = makeExtractor(next, mapOfNamesAndTypes, mapOfCols);
                builder.withExtractor(next.getName(), extractor);
            }
            GeneratedRowWriter rowWriter = builder.build();

            try {
                forEachExportedRow(exportBucket, s3ObjectKey, schemaName, mapOfNamesAndTypes, row -> {
                    rowContext.setNameValue(row);
                    spiller.writeRows((Block block, int rowNum) -> rowWriter.writeRow(block, rowNum, rowContext) ? 1 : 0);
                });
            } catch (Exception e) {
                if (e instanceof IOException) {
                    throw (IOException) e;
                }
                throw new IOException("Error reading Parquet export from S3 object: " + s3ObjectKey, e);
            }
        }
    }

    /**
     * Reads a Vertica-exported Parquet object from S3 via Hadoop S3A (seekable, no local temp file) and invokes
     * {@code rowConsumer} per row. Pure-Java Parquet (no Arrow Dataset JNI) for Lambda Amazon Linux 2 / glibc 2.26.
     * <p>
     * S3A (Hadoop 3.4+) uses AWS SDK v2 {@link software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider}
     * (same default chain as Lambda: env, profile, web identity, container credentials, etc.).
     */
    @VisibleForTesting
    void forEachExportedRow(String exportBucket, String s3ObjectKey, Schema schemaName,
                            Map<String, Types.MinorType> mapOfNamesAndTypes, Consumer<Map<String, Object>> rowConsumer) throws Exception
    {
        streamRowsFromS3(exportBucket, s3ObjectKey, schemaName, mapOfNamesAndTypes, rowConsumer);
    }

    /**
     * Vertica Parquet decimals may be wider than the Athena/Glue column type. Arrow {@code DecimalVector}
     * rejects values whose precision exceeds the vector; clamp to DECIMAL(precision, scale) range.
     */
    @VisibleForTesting
    static BigDecimal normalizeToArrowDecimal(BigDecimal raw, int precision, int scale, String fieldNameForLog)
    {
        BigDecimal scaledDecimal = raw.setScale(scale, RoundingMode.HALF_UP);
        BigInteger maxUnscaled = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
        BigInteger minUnscaled = maxUnscaled.negate();
        BigInteger unscaledMagnitude = scaledDecimal.unscaledValue();
        if (unscaledMagnitude.compareTo(maxUnscaled) <= 0 && unscaledMagnitude.compareTo(minUnscaled) >= 0) {
            return scaledDecimal;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("normalizeToArrowDecimal: clamping field[{}] value[{}] to DECIMAL({},{})", fieldNameForLog, raw, precision, scale);
        }
        BigDecimal maxVal = new BigDecimal(maxUnscaled, scale);
        BigDecimal minVal = new BigDecimal(minUnscaled, scale);
        if (scaledDecimal.compareTo(maxVal) > 0) {
            return maxVal;
        }
        if (scaledDecimal.compareTo(minVal) < 0) {
            return minVal;
        }
        return scaledDecimal;
    }

    private static Configuration s3aConfigurationForLambda()
    {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.setBoolean("fs.s3a.impl.disable.cache", true);
        conf.set("fs.s3a.aws.credentials.provider",
                software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider.class.getName());
        String region = System.getenv("AWS_REGION");
        if (region == null || region.isEmpty()) {
            region = System.getenv("AWS_DEFAULT_REGION");
        }
        if (region != null && !region.isEmpty()) {
            conf.set("fs.s3a.endpoint.region", region);
        }
        return conf;
    }

    private static Path s3aObjectPath(String bucket, String key)
    {
        String normalizedKey = key.startsWith("/") ? key.substring(1) : key;
        return new Path("s3a", bucket, "/" + normalizedKey);
    }

    static void streamRowsFromS3(String bucket, String key, Schema schemaName,
                                 Map<String, Types.MinorType> mapOfNamesAndTypes, Consumer<Map<String, Object>> rowConsumer) throws Exception
    {
        Path path = s3aObjectPath(bucket, key);
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(s3aConfigurationForLambda())
                .build()) {
            Group group;
            while ((group = reader.read()) != null) {
                rowConsumer.accept(toRowMap(group, schemaName, mapOfNamesAndTypes));
            }
        }
    }

    private static Map<String, Object> toRowMap(Group group, Schema schemaName, Map<String, Types.MinorType> mapOfNamesAndTypes)
    {
        Map<String, Object> map = new HashMap<>();
        for (Field field : schemaName.getFields()) {
            String name = field.getName();
            Types.MinorType minorType = mapOfNamesAndTypes.get(name);
            map.put(name, extractValue(group, name, field, minorType));
        }
        return map;
    }

    /**
     * VARCHAR in metadata may not match Parquet physical type; use {@link Group#getValueToString} to avoid wrong getters.
     */
    private static String varcharFromParquetPrimitive(Group group, String pqField)
    {
        GroupType schema = group.getType();
        Type fieldType = schema.getType(pqField);
        if (!fieldType.isPrimitive()) {
            throw new IllegalStateException("Expected primitive Parquet column for VARCHAR mapping: " + pqField);
        }
        return group.getValueToString(schema.getFieldIndex(pqField), 0);
    }

    private static Object extractValue(Group group, String pqField, Field arrowField, Types.MinorType minorType)
    {
        if (group.getFieldRepetitionCount(pqField) == 0) {
            return null;
        }
        switch (minorType) {
            case BIT:
                return group.getBoolean(pqField, 0);
            case TINYINT:
                return (byte) group.getInteger(pqField, 0);
            case SMALLINT:
                return (short) group.getInteger(pqField, 0);
            case INT:
                return group.getInteger(pqField, 0);
            case BIGINT:
                return group.getLong(pqField, 0);
            case FLOAT4:
                return group.getFloat(pqField, 0);
            case FLOAT8:
                return group.getDouble(pqField, 0);
            case DECIMAL: {
                ArrowType type = arrowField.getType();
                if (!(type instanceof ArrowType.Decimal)) {
                    throw new IllegalStateException("Expected DECIMAL for field " + arrowField.getName());
                }
                ArrowType.Decimal d = (ArrowType.Decimal) type;
                Binary bin = group.getBinary(pqField, 0);
                BigDecimal raw = new BigDecimal(new BigInteger(bin.getBytesUnsafe()), d.getScale());
                return VerticaRecordHandler.normalizeToArrowDecimal(raw, d.getPrecision(), d.getScale(), arrowField.getName());
            }
            case VARCHAR:
                return varcharFromParquetPrimitive(group, pqField);
            case VARBINARY:
                return group.getBinary(pqField, 0).getBytes();
            case DATEDAY:
                return group.getInteger(pqField, 0);
            case DATEMILLI:
                return group.getLong(pqField, 0);
            default:
                throw new IllegalStateException("Unsupported type for Parquet export read: " + minorType + " field " + arrowField.getName());
        }
    }

    /**
     * Creates an Extractor for the given field.
     */
    private Extractor makeExtractor(Field field, HashMap<String, Types.MinorType> mapOfNamesAndTypes, HashMap<String, Object> mapOfcols)
    {
        String fieldName = field.getName();
        Types.MinorType fieldType = mapOfNamesAndTypes.get(fieldName);
        switch (fieldType)
        {
            case BIT:
                return (BitExtractor) (Object context, NullableBitHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if(value == null)
                    {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = ((boolean) value) ? 1 : 0;
                        dst.isSet = 1;
                        }
                };
            case TINYINT:
                return (TinyIntExtractor) (Object context, NullableTinyIntHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if(value == null)
                    {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = Byte.parseByte(value.toString());
                        dst.isSet = 1;
                    }
                };
            case SMALLINT:
                return (SmallIntExtractor) (Object context, NullableSmallIntHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if(value == null)
                    {
                        dst.isSet = 0;
                    }
                    else{
                        dst.value = Short.parseShort(value.toString());
                        dst.isSet = 1;
                    }
                };
            case INT:
            case BIGINT:
                return (BigIntExtractor) (Object context, NullableBigIntHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if(value == null){
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = Long.parseLong(value.toString());
                        dst.isSet = 1;
                    }
                };
            case FLOAT4:
                return (Float4Extractor) (Object context, NullableFloat4Holder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if(value == null){
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = Float.parseFloat(value.toString());
                        dst.isSet = 1;
                    }
                };
            case FLOAT8:
                return (Float8Extractor) (Object context, NullableFloat8Holder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if(value == null){
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = Double.parseDouble(value.toString());
                        dst.isSet = 1;
                    }
                };
            case DECIMAL:
                return (DecimalExtractor) (Object context, NullableDecimalHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if(value == null)
                    {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = new BigDecimal(value.toString());
                        dst.isSet = 1;
                    }

                };
            case DATEDAY:
                return (DateDayExtractor) (Object context, NullableDateDayHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if(value == null){
                        dst.isSet = 0;
                    }
                    else{
                        dst.isSet = 1;
                        dst.value = (int) LocalDate.parse(value.toString()).toEpochDay();
                    }

                };

            case DATEMILLI:
                return (DateMilliExtractor) (Object context, NullableDateMilliHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName).toString();
                    if(value == null)
                    {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = LocalDateTime.parse(value.toString()).atZone(BlockUtils.UTC_ZONE_ID).toInstant().toEpochMilli();
                        dst.isSet = 1;
                    }
                };
            case VARCHAR:
                return (VarCharExtractor) (Object context, NullableVarCharHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName);
                    if(value == null)
                    {
                        dst.isSet = 0;
                    }
                    else{
                        dst.value = value.toString();
                        dst.isSet = 1;
                    }
                };
            case VARBINARY:
                return (VarBinaryExtractor) (Object context, NullableVarBinaryHolder dst) ->
                {
                    Object value = ((RowContext) context).getNameValue().get(fieldName).toString();
                    if(value == null)
                    {
                        dst.isSet = 0;
                    }
                    else {
                        dst.value = value.toString().getBytes();
                        dst.isSet = 1;
                    }
                };
            default:
                throw new RuntimeException("Unhandled type " + fieldType);
        }
    }

    private static class RowContext
    {
        private final String queryId;
        private HashMap<String, Object> nameValue;

        public RowContext(String queryId){
            this.queryId = queryId;
        }

        public void setNameValue(Map<String, Object> map){
            this.nameValue = new HashMap<>(map);
        }
        public HashMap<String, Object> getNameValue() {
            return this.nameValue;
        }
    }
}
