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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.jembi.jempi.AppConfig.KAFKA_CLIENT_ID;

class SyncPatientsStream {

    private static final Logger LOGGER = LogManager.getLogger(SyncPatientsStream.class);
    private final NdwDao ndwDao;
    private final NotificationDao notificationDao;
    private KafkaStreams patientSyncStream;
    private MyKafkaProducer<String, InteractionEnvelop> interactionEnvelopProducer;

    SyncPatientsStream() {
        LOGGER.info("SyncPatientsStream constructor");
        ndwDao = new NdwDao();
        notificationDao = new NotificationDao();
    }

    static SyncPatientsStream create() {
        return new SyncPatientsStream();
    }

    private void processPatientListResult(final String key,
                                          final SyncEvent event) {
        LOGGER.info("Processing event {}, {}, {}", event.event(), key, event.createdAt().toString());
        try {
            List<CustomPatientRecord> patientRecordList = ndwDao.getPatientList(key, event);
            LOGGER.info("Syncing {} patient records", patientRecordList.size());
            if (!patientRecordList.isEmpty()) {
                final var dtf = DateTimeFormatter.ofPattern("uuuu/MM/dd HH:mm:ss");
                final var now = LocalDateTime.now();
                final var stanDate = dtf.format(now);
                final var uuid = UUID.randomUUID().toString();
                int index = 0;
                sendToKafka(uuid, new InteractionEnvelop(InteractionEnvelop.ContentType.BATCH_START_SENTINEL, "patient-sync",
                        String.format(Locale.ROOT, "%s:%07d", stanDate, ++index), null));
                for (CustomPatientRecord patient : patientRecordList) {
                    LOGGER.debug("Persisting record {}", patient);
                    CustomUniqueInteractionData uniqueInteractionData = new CustomUniqueInteractionData(java.time.LocalDateTime.now(),
                            null, patient.pkv(), null);
                    CustomDemographicData demographicData = new CustomDemographicData(null, null,
                            patient.gender(), patient.dob() == null ? null : patient.dob().toString(),
                            patient.nupi(), patient.cccNumber(), patient.docket());
                    CustomSourceId sourceId = new CustomSourceId(null, patient.siteCode(), patient.patientPk());
                    String dwhId = notificationDao.insertClinicalData(demographicData, sourceId, uniqueInteractionData);
                    if (dwhId != null) {
                        uniqueInteractionData = new CustomUniqueInteractionData(uniqueInteractionData.auxDateCreated(),
                                null, uniqueInteractionData.pkv(), dwhId);
                        sendToKafka(UUID.randomUUID().toString(),
                                new InteractionEnvelop(InteractionEnvelop.ContentType.BATCH_INTERACTION, "patient-sync",
                                        String.format(Locale.ROOT, "%s:%07d", stanDate, ++index),
                                        new Interaction(null,
                                                sourceId,
                                                uniqueInteractionData,
                                                demographicData)));
                        LOGGER.info("Inserted & queued record with dwhId {}, index {}", uniqueInteractionData.auxDwhId(), index);
                    } else {
                        LOGGER.error("Failed to insert record sc({}) pk({})", sourceId.facility(), sourceId.patient());
                    }
                }
                sendToKafka(uuid, new InteractionEnvelop(InteractionEnvelop.ContentType.BATCH_END_SENTINEL, "patient-sync",
                        String.format(Locale.ROOT, "%s:%07d", stanDate, ++index), null));
                LOGGER.info("Patient sync complete.");
            }
//        } catch (InterruptedException | ExecutionException ex) {
        } catch (Exception ex) {
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
                KAFKA_CLIENT_ID);

        interactionEnvelopProducer = new MyKafkaProducer<>(AppConfig.KAFKA_BOOTSTRAP_SERVERS,
                GlobalConstants.TOPIC_INTERACTION_ETL,
                new StringSerializer(), new JsonPojoSerializer<>(),
                KAFKA_CLIENT_ID);

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
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.KAFKA_BOOTSTRAP_SERVERS);
        return props;
    }
}
