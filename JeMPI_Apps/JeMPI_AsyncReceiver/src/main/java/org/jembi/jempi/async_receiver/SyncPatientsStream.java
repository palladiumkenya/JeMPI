package org.jembi.jempi.async_receiver;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jembi.jempi.AppConfig;
import org.jembi.jempi.shared.kafka.MyKafkaProducer;
import org.jembi.jempi.shared.models.*;
import org.jembi.jempi.shared.serdes.JsonPojoDeserializer;
import org.jembi.jempi.shared.serdes.JsonPojoSerializer;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

class SyncPatientsStream {

    private static final Logger LOGGER = LogManager.getLogger(SyncPatientsStream.class);
    private final DWH dwh;
    private KafkaStreams patientSyncStream;


    SyncPatientsStream() {
        LOGGER.info("SyncPatientsStream constructor");
        dwh = new DWH();
    }

    static SyncPatientsStream create() {
        return new SyncPatientsStream();
    }

    private void processPatientListResult(final String key,
                                          final SyncEvent event) {
        LOGGER.info("Processing event {}, {}, {}", event.event(), key, event.createdAt().toString());
        try {
            dwh.syncPatientList(key, event);
        } catch (InterruptedException | ExecutionException ex) {
            LOGGER.error(ex.getLocalizedMessage(), ex);
            close();
        }
    }

    void open() {
        LOGGER.info("KAFKA: {} {} {}",
                AppConfig.KAFKA_BOOTSTRAP_SERVERS,
                "dwh-async-application-id",
                AppConfig.KAFKA_CLIENT_ID);
//        LOGGER.info("KAFKA: {} {} {}",
//                AppConfig.KAFKA_BOOTSTRAP_SERVERS,
//                AppConfig.KAFKA_APPLICATION_ID,
//                AppConfig.KAFKA_CLIENT_ID);
//        interactionEnvelopProducer = new MyKafkaProducer<>(AppConfig.KAFKA_BOOTSTRAP_SERVERS,
//                GlobalConstants.TOPIC_INTERACTION_ASYNC_ETL,
//                new StringSerializer(), new JsonPojoSerializer<>(),
//                "dwh-async-client-id-syncrx");

        dwh.initiateProducer();
        final Properties props = loadConfig();
        final Serde<String> stringSerde = Serdes.String();
        final Serde<SyncEvent> muSerde = Serdes.serdeFrom(new JsonPojoSerializer<>(),
                new JsonPojoDeserializer<>(SyncEvent.class));
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<String, SyncEvent> muStream = streamsBuilder.stream(
                GlobalConstants.TOPIC_SYNC_PATIENTS_DWH,
                Consumed.with(stringSerde, muSerde));
        muStream.foreach(this::processPatientListResult);
        patientSyncStream = new KafkaStreams(streamsBuilder.build(), props);
        patientSyncStream.cleanUp();
        patientSyncStream.start();
        Runtime.getRuntime().addShutdownHook(new Thread(patientSyncStream::close));
        LOGGER.info("Sync Patients KafkaStreams started");
    }

    void close() {
        LOGGER.warn("Sync Patients Stream closed");
        patientSyncStream.close();
    }

    private Properties loadConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "patient-sync-application-id");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "client-id-patientsyncrx");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.KAFKA_BOOTSTRAP_SERVERS);
        return props;
    }
}
