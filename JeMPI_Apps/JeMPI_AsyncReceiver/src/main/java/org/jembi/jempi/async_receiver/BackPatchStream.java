//package org.jembi.jempi.async_receiver;
//
//import org.apache.kafka.common.serialization.Serde;
//import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.streams.KafkaStreams;
//import org.apache.kafka.streams.StreamsBuilder;
//import org.apache.kafka.streams.StreamsConfig;
//import org.apache.kafka.streams.kstream.Consumed;
//import org.apache.kafka.streams.kstream.KStream;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.jembi.jempi.AppConfig;
//import org.jembi.jempi.shared.models.BackPatchDWH;
//import org.jembi.jempi.shared.models.GlobalConstants;
//import org.jembi.jempi.shared.serdes.JsonPojoDeserializer;
//import org.jembi.jempi.shared.serdes.JsonPojoSerializer;
//
//import java.util.Properties;
//
//class BackPatchStream {
//   private static final Logger LOGGER = LogManager.getLogger(BackPatchStream.class);
//   private final DWH dwh;
//   private KafkaStreams backPatchStreams;
//
//
//   BackPatchStream() {
//      LOGGER.info("BackPatchStream constructor");
//      dwh = new DWH();
//   }
//
//   static BackPatchStream create() {
//      return new BackPatchStream();
//   }
//
//   private void backPatch(
//         final String key,
//         final BackPatchDWH rec) {
//      LOGGER.debug("{} - {}", key, rec);
//      dwh.backPatchKeys(rec.dwhId(), rec.goldenId(), rec.encounterId());
//   }
//
//   void open() {
//      final Properties props = loadConfig();
//      final Serde<String> stringSerde = Serdes.String();
//      final Serde<BackPatchDWH> muSerde = Serdes.serdeFrom(new JsonPojoSerializer<>(),
//                                                           new JsonPojoDeserializer<>(BackPatchDWH.class));
//      final StreamsBuilder streamsBuilder = new StreamsBuilder();
//      final KStream<String, BackPatchDWH> muStream = streamsBuilder.stream(
//            GlobalConstants.TOPIC_BACK_PATCH_DWH,
//            Consumed.with(stringSerde, muSerde));
//      muStream.foreach(this::backPatch);
//      backPatchStreams = new KafkaStreams(streamsBuilder.build(), props);
//      backPatchStreams.cleanUp();
//      backPatchStreams.start();
//      LOGGER.info("KafkaStreams started");
//   }
//
//   void close() {
//      LOGGER.warn("Stream closed");
//      backPatchStreams.close();
//   }
//
//   private Properties loadConfig() {
//      final Properties props = new Properties();
//      props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfig.KAFKA_APPLICATION_ID);
//      props.put(StreamsConfig.CLIENT_ID_CONFIG, AppConfig.KAFKA_CLIENT_ID);
//      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.KAFKA_BOOTSTRAP_SERVERS);
//      return props;
//   }
//
//}
