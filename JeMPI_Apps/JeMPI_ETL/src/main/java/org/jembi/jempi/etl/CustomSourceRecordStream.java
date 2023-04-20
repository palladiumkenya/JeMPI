package org.jembi.jempi.etl;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.codec.language.Metaphone;
import org.apache.commons.codec.language.RefinedSoundex;
import org.apache.commons.codec.language.Soundex;
// import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jembi.jempi.AppConfig;
import org.jembi.jempi.shared.models.*;
import org.jembi.jempi.shared.serdes.JsonPojoDeserializer;
import org.jembi.jempi.shared.serdes.JsonPojoSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class CustomSourceRecordStream {

   private static final Logger LOGGER = LogManager.getLogger(CustomSourceRecordStream.class);
   ExecutorService executorService = Executors.newFixedThreadPool(1);
   private KafkaStreams patientKafkaStreams = null;

   public void open() {

      final Properties props = loadConfig();
      final Serde<String> stringSerde = Serdes.String();
      final Serializer<AsyncSourceRecord> sourceRecordSerializer = new JsonPojoSerializer<>();
      final Deserializer<AsyncSourceRecord> sourceRecordDeserializer = new JsonPojoDeserializer<>(AsyncSourceRecord.class);
      final Serializer<BatchPatientRecord> batchPatientSerializer = new JsonPojoSerializer<>();
      final Deserializer<BatchPatientRecord> batchPatientDeserializer = new JsonPojoDeserializer<>(BatchPatientRecord.class);
      final Serde<AsyncSourceRecord> sourceRecordSerde = Serdes.serdeFrom(sourceRecordSerializer, sourceRecordDeserializer);
      final Serde<BatchPatientRecord> batchPatientSerde = Serdes.serdeFrom(batchPatientSerializer, batchPatientDeserializer);
      final StreamsBuilder streamsBuilder = new StreamsBuilder();
      final KStream<String, AsyncSourceRecord> patientKStream =
            streamsBuilder.stream(GlobalConstants.TOPIC_PATIENT_ASYNC_ETL, Consumed.with(stringSerde, sourceRecordSerde));
      patientKStream.map((key, rec) -> {
                       LOGGER.debug("{} {}", key, rec);
                       var batchType = switch (rec.recordType().type) {
                          case AsyncSourceRecord.RecordType.BATCH_START_VALUE -> BatchPatientRecord.BatchType.BATCH_START;
                          case AsyncSourceRecord.RecordType.BATCH_END_VALUE -> BatchPatientRecord.BatchType.BATCH_END;
                          default -> BatchPatientRecord.BatchType.BATCH_PATIENT;
                       };
                       if (batchType == BatchPatientRecord.BatchType.BATCH_PATIENT) {
                        //   var k = rec.customSourceRecord().phoneticFamilyName();
                        //   if (StringUtils.isBlank(k)) {
                        //      k = "anon";
                        //   }
                        //   k = switch (AppConfig.KAFKA_KEY_ENCODER) {
                        //      case "None" -> key;
                        //      case "SoundEx" -> getEncodedMF(k, OperationType.OPERATION_TYPE_SOUNDEX);
                        //      default -> getEncodedMF(k, OperationType.OPERATION_TYPE_DOUBLE_METAPHONE);
                        //   };
                          var batchPatient = new BatchPatientRecord(batchType,
                                                                    rec.batchMetaData(),
                                                                    rec.customSourceRecord().stan(),
                                                                    new PatientRecord(null,
                                                                                      rec.customSourceRecord().sourceId(),
                                                                                      new CustomDemographicData(
                                                                                            rec.customSourceRecord().auxId(),
                                                                                            rec.customSourceRecord().auxDwhId(),
                                                                                            rec.customSourceRecord().phoneticGivenName(),
                                                                                            rec.customSourceRecord().phoneticFamilyName(),
                                                                                            rec.customSourceRecord().gender(),
                                                                                            rec.customSourceRecord().dob(),
                                                                                            rec.customSourceRecord().nupi())));
                          LOGGER.info("{} : {}", key, batchPatient);
                          return KeyValue.pair(key, batchPatient);
                       } else {
                          return KeyValue.pair("SENTINEL", new BatchPatientRecord(batchType, rec.batchMetaData(), null, null));
                       }
                    })
                    .filter((key, value) -> (value.batchType() == BatchPatientRecord.BatchType.BATCH_PATIENT))
                    .to(GlobalConstants.TOPIC_PATIENT_CONTROLLER, Produced.with(stringSerde, batchPatientSerde));
      patientKafkaStreams = new KafkaStreams(streamsBuilder.build(), props);
      patientKafkaStreams.cleanUp();
      patientKafkaStreams.start();
   }

   private String getEncodedMF(
         final String value,
         final OperationType algorithmType) {
      return switch (algorithmType) {
         case OPERATION_TYPE_NONE -> value;
         case OPERATION_TYPE_METAPHONE -> (new Metaphone()).metaphone(value);
         case OPERATION_TYPE_DOUBLE_METAPHONE -> (new DoubleMetaphone()).doubleMetaphone(value);
         case OPERATION_TYPE_SOUNDEX -> (new Soundex()).encode(value);
         case OPERATION_TYPE_REFINED_SOUNDEX -> (new RefinedSoundex()).encode(value);
      };
   }

   public void close() {
      patientKafkaStreams.close();
   }

   private Properties loadConfig() {
      final Properties props = new Properties();
      props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfig.KAFKA_APPLICATION_ID);
      props.put(StreamsConfig.CLIENT_ID_CONFIG, AppConfig.KAFKA_CLIENT_ID);
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.KAFKA_BOOTSTRAP_SERVERS);
      props.put(StreamsConfig.POLL_MS_CONFIG, 10);
      return props;
   }

   public enum OperationType {
      OPERATION_TYPE_NONE, OPERATION_TYPE_METAPHONE, OPERATION_TYPE_DOUBLE_METAPHONE, OPERATION_TYPE_SOUNDEX,
      OPERATION_TYPE_REFINED_SOUNDEX
   }

}
