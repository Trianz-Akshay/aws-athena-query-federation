package com.amazonaws.athena.connectors.msk;

import com.amazonaws.services.glue.model.DataFormat;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class AvroProducerWithSASL {
    public static void main(String[] args) {
        // Define the Avro schema
        String schemaString = "{\"type\":\"record\",\"name\":\"ExampleRecord\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(schemaString);

        // Set up Kafka producer properties
//        Properties props = new Properties();
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "b-1.msksaslscram.hic76u.c7.kafka.us-west-2.amazonaws.com:9092");
//        props.put("key.serializer", KafkaAvroSerializer.class.getName());
//        props.put("value.serializer", KafkaAvroSerializer.class.getName());
//        //props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
//        props.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
//        props.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
//        props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "test-new-msk-version");
//        props.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "ExampleRecord");
//        // SASL/SCRAM configuration
//        props.put("security.protocol", "SASL_SSL");
//        props.put("sasl.mechanism", "SCRAM-SHA-512");
//        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"Trianz123\";");
//
//        // Create Kafka producer
//        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);
//
//        // Create a sample Avro record
//        GenericRecord record = new GenericData.Record(schema);
//        record.put("field1", "value1");
//
//        // Create a ProducerRecord and send it
//        ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>("example-topic", record);
//        producer.send(producerRecord);
//
//        // Close the producer
//        producer.close();
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "b-2.msksaslscram.hic76u.c7.kafka.us-west-2.amazonaws.com:9096");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaSerializer.class.getName());
        properties.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
        properties.put(AWSSchemaRegistryConstants.AWS_REGION, "us-west-2");
        properties.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "test-new-msk-version");
        properties.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "ExampleRecord");

//        Schema schema_payment = null;
//        try {
//            schema_payment = parser.parse(new File("src/main/resources/avro/com/tutorial/Payment.avsc"));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        GenericRecord musical = new GenericData.Record(schema);
        musical.put("field1", "entertainment_1");
        GenericRecord musical1 = new GenericData.Record(schema);
        musical1.put("field1", "entertainment_2");

        List<GenericRecord> misc = new ArrayList<>();

        misc.add(musical);
        misc.add(musical1);

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(properties)) {
            String topic = "example-topic1";
            for (int i = 0; i < 4; i++) {
                GenericRecord r = misc.get(i);

                final ProducerRecord<String, GenericRecord> record;
                record = new ProducerRecord<String, GenericRecord>(topic, r.get("field1").toString(), r);

                producer.send(record);
                System.out.println("Sent message " + i);
                Thread.sleep(1000L);
            }
            producer.flush();
            System.out.println("Successfully produced 10 messages to a topic called " + topic);

        } catch (final InterruptedException | SerializationException e) {
            e.printStackTrace();
        }
    }
}
