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
import org.jembi.jempi.shared.models.*;
import org.jembi.jempi.shared.serdes.JsonPojoDeserializer;
import org.jembi.jempi.shared.serdes.JsonPojoSerializer;

import java.util.Properties;

import static org.jembi.jempi.AppConfig.KAFKA_CLIENT_ID;

class MatchNotificationsStream {
    private static final Logger LOGGER = LogManager.getLogger(SyncPatientsStream.class);
    private final NotificationDao notificationDao;
    private KafkaStreams matchNotificationDataStream;

    MatchNotificationsStream() {
        LOGGER.info("SyncPatientsStream constructor");
        notificationDao = new NotificationDao();
    }

    static MatchNotificationsStream create() {
        return new MatchNotificationsStream();
    }

    private void processMatchNotifications(final String key,
                                          final MatchCandidatesData matchCandidatesData) {
        LOGGER.info("Processing candidates for interaction dwh: {}", key);
        matchCandidatesData.candidates().forEach(candidate -> notificationDao.insertMatchingNotifications(candidate, matchCandidatesData.interaction(),
                matchCandidatesData.topCandidateGoldenId().equals(candidate.goldenId())));
    }

    void open() {
        LOGGER.info("KAFKA: {} {} {}",
                AppConfig.KAFKA_BOOTSTRAP_SERVERS,
                KAFKA_CLIENT_ID);

        final Properties props = loadConfig();
        final Serde<String> stringSerde = Serdes.String();
        final Serde<MatchCandidatesData> matchNotificationserde = Serdes.serdeFrom(new JsonPojoSerializer<>(),
                new JsonPojoDeserializer<>(MatchCandidatesData.class));
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<String, MatchCandidatesData> muStream = streamsBuilder.stream(
                GlobalConstants.TOPIC_MATCH_DATA_DWH,
                Consumed.with(stringSerde, matchNotificationserde));
        muStream.foreach(this::processMatchNotifications);
        matchNotificationDataStream = new KafkaStreams(streamsBuilder.build(), props);
        matchNotificationDataStream.cleanUp();
        matchNotificationDataStream.start();
        Runtime.getRuntime().addShutdownHook(new Thread(matchNotificationDataStream::close));
        LOGGER.info("Match Notifications KafkaStreams started");
    }

    void close() {
        LOGGER.warn("Match Notifications Stream closed");
        matchNotificationDataStream.close();
    }

    private Properties loadConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "match-notification-application-id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.KAFKA_BOOTSTRAP_SERVERS);
        return props;
    }
}
