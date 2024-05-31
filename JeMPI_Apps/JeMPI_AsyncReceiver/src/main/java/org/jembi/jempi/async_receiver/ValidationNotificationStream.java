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
import org.jembi.jempi.shared.models.GlobalConstants;
import org.jembi.jempi.shared.models.Interaction;
import org.jembi.jempi.shared.serdes.JsonPojoDeserializer;
import org.jembi.jempi.shared.serdes.JsonPojoSerializer;

import java.util.Properties;

class ValidationNotificationStream {
    private static final Logger LOGGER = LogManager.getLogger(ValidationNotificationStream.class);
    private final NotificationDao notificationDao;
    private KafkaStreams validationStream;

    ValidationNotificationStream() {
        LOGGER.info("ValidationStream constructor");
        notificationDao = new NotificationDao();
    }

    static ValidationNotificationStream create() {
        return new ValidationNotificationStream();
    }

    private void insertValidation(
            final String key,
            final Interaction rec) {
        LOGGER.debug("{} - {}", key, rec);
        try {
            notificationDao.insertValidationNotification(rec.uniqueInteractionData().auxDwhId());
        } catch (Exception e) {
            close();
            LOGGER.error(e.getLocalizedMessage(), e);
        }
    }

    void open() {
        final Properties props = loadConfig();
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Interaction> muSerde = Serdes.serdeFrom(new JsonPojoSerializer<>(),
                new JsonPojoDeserializer<>(Interaction.class));
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<String, Interaction> vlStream = streamsBuilder.stream(
                GlobalConstants.TOPIC_VALIDATION_DATA_DWH,
                Consumed.with(stringSerde, muSerde));
        vlStream.foreach(this::insertValidation);
        validationStream = new KafkaStreams(streamsBuilder.build(), props);
        validationStream.cleanUp();
        validationStream.start();
        Runtime.getRuntime().addShutdownHook(new Thread(validationStream::close));
        LOGGER.info("KafkaStreams started");
    }

    void close() {
        LOGGER.warn("Validations Stream closed");
        validationStream.close();
    }

    private Properties loadConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "async-receiver-Validation-app-id");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "client-id-2763df");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.KAFKA_BOOTSTRAP_SERVERS);
        return props;
    }
}
