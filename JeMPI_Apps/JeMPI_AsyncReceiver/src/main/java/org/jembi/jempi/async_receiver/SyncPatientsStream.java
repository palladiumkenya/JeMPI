package org.jembi.jempi.async_receiver;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

class SyncPatientsStream {

    private static final Logger LOGGER = LogManager.getLogger(SyncPatientsStream.class);
    private final DWH dwh;
    private KafkaStreams patientSyncStream;
    private MyKafkaProducer<String, InteractionEnvelop> interactionEnvelopProducer;


    SyncPatientsStream() {
        LOGGER.info("SyncPatientsStream constructor");
        dwh = new DWH();
    }

    static SyncPatientsStream create() {
        return new SyncPatientsStream();
    }

    private void processPatientListResult(final String key,
                                          final SyncEvent event) {
        LOGGER.info("Processing event {}, {}", event.event(), event.createdAt().toString());
        try {
            ResultSet resultSet = dwh.getPatientList();
            if (resultSet != null) {
                final var dtf = DateTimeFormatter.ofPattern("uuuu/MM/dd HH:mm:ss");
                final var now = LocalDateTime.now();
                final var stanDate = dtf.format(now);
                final var uuid = UUID.randomUUID().toString();
                int index = 0;
                sendToKafka(uuid, new InteractionEnvelop(InteractionEnvelop.ContentType.BATCH_START_SENTINEL, stanDate,
                        String.format(Locale.ROOT, "%s:%07d", stanDate, ++index), null));
                while (resultSet.next()) {
                    CustomUniqueInteractionData uniqueInteractionData = new CustomUniqueInteractionData(java.time.LocalDateTime.now(),
                            null, resultSet.getString("CCCNumber"), resultSet.getString("docket"),
                            resultSet.getString("PKV"), null);
                    CustomDemographicData demographicData = new CustomDemographicData(null, null,
                            resultSet.getString("Gender"), resultSet.getDate("DOB").toString(),
                            resultSet.getString("NUPI"));
                    CustomSourceId sourceId = new CustomSourceId(null, "SiteCode", "PatientPK");
                    String dwhId = dwh.insertClinicalData(demographicData, sourceId, uniqueInteractionData);

                    if (dwhId == null) {
                        LOGGER.warn("Failed to insert record sc({}) pk({})", sourceId.facility(), sourceId.patient());
                    }
                    LOGGER.debug("Inserted record with dwhId {}", dwhId);
                    sendToKafka(UUID.randomUUID().toString(),
                            new InteractionEnvelop(InteractionEnvelop.ContentType.BATCH_INTERACTION, stanDate,
                                    String.format(Locale.ROOT, "%s:%07d", stanDate, ++index),
                                    new Interaction(null,
                                            sourceId,
                                            uniqueInteractionData,
                                            demographicData)));
                }
                sendToKafka(uuid, new InteractionEnvelop(InteractionEnvelop.ContentType.BATCH_END_SENTINEL, stanDate,
                        String.format(Locale.ROOT, "%s:%07d", stanDate, ++index), null));
                int patientCount = index - 2;
                LOGGER.info("Synced {} patient records", patientCount);
            }
        } catch (InterruptedException | ExecutionException | SQLException ex) {
            LOGGER.error(ex.getLocalizedMessage(), ex);
            close();
        }
    }

    private void sendToKafka(
            final String key,
            final InteractionEnvelop interactionEnvelop)
            throws InterruptedException, ExecutionException {
        try {
            interactionEnvelopProducer.produceSync(key, interactionEnvelop);
        } catch (NullPointerException ex) {
            LOGGER.error(ex.getLocalizedMessage(), ex);
        }
    }

    void open() {
        LOGGER.info("KAFKA: {} {} {}",
                AppConfig.KAFKA_BOOTSTRAP_SERVERS,
                AppConfig.KAFKA_APPLICATION_ID,
                AppConfig.KAFKA_CLIENT_ID);
        interactionEnvelopProducer = new MyKafkaProducer<>(AppConfig.KAFKA_BOOTSTRAP_SERVERS,
                GlobalConstants.TOPIC_INTERACTION_ASYNC_ETL,
                new StringSerializer(), new JsonPojoSerializer<>(),
                AppConfig.KAFKA_CLIENT_ID);

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
