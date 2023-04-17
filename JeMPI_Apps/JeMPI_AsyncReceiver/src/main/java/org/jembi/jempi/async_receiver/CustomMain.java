package org.jembi.jempi.async_receiver;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.codec.language.Metaphone;
import org.apache.commons.codec.language.RefinedSoundex;
import org.apache.commons.codec.language.Soundex;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jembi.jempi.AppConfig;
import org.jembi.jempi.shared.kafka.MyKafkaProducer;
import org.jembi.jempi.shared.models.*;
import org.jembi.jempi.shared.serdes.JsonPojoSerializer;
import scala.Tuple3;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.file.StandardWatchEventKinds.*;

// @formatter:off
// @formatter:on

public final class CustomMain {

   private static final Logger LOGGER = LogManager.getLogger(CustomMain.class.getName());
   private MyKafkaProducer<String, AsyncSourceRecord> sourceRecordProducer;
   private DWH dwh;

   public static void main(final String[] args)
         throws InterruptedException, ExecutionException, IOException {
      new CustomMain().run();
   }

   @SuppressWarnings("unchecked")
   private static <T> WatchEvent<T> cast(final WatchEvent<?> event) {
      return (WatchEvent<T>) event;
   }

   private static Tuple3<OperationType, OperationType, Float> parseFileName(final String fileName) {
      final String regex = "gn_(?<gn>\\w*)_fn_(?<fn>\\w*)_th_(?<th>\\d*[.]\\d+).csv$";
      final Pattern pattern = Pattern.compile(regex);
      final Matcher matcher = pattern.matcher(fileName);
      if (matcher.find()) {
         final var gn = matcher.group("gn");
         final var fn = matcher.group("fn");
         final var th = matcher.group("th");
         OperationType gnOperationType = switch (gn.toLowerCase()) {
            case "s" -> OperationType.OPERATION_TYPE_SOUNDEX;
            case "d" -> OperationType.OPERATION_TYPE_DOUBLE_METAPHONE;
            default -> null;
         };
         OperationType fnOperationType = switch (fn.toLowerCase()) {
            case "s" -> OperationType.OPERATION_TYPE_SOUNDEX;
            case "d" -> OperationType.OPERATION_TYPE_DOUBLE_METAPHONE;
            default -> null;
         };
         return gnOperationType != null && fnOperationType != null && th != null
               ? new Tuple3<>(gnOperationType, fnOperationType, Float.parseFloat(th))
               : null;
      }
      return null;
   }

   private static SourceId parseSourceId(final String sourceId) {
      final String regex = "^(\\w+):(\\w+)$";
      final Pattern pattern = Pattern.compile(regex);
      final Matcher matcher = pattern.matcher(sourceId);
      if (matcher.find()) {
         final var facility = matcher.group(1);
         final var patient = matcher.group(2);
         return facility != null && patient != null
               ? new SourceId(null, facility, patient)
               : null;
      }
      return null;
   }

   private void sendToKafka(
         final String key,
         final AsyncSourceRecord asyncSourceRecord)
         throws InterruptedException, ExecutionException {
      try {
         LOGGER.debug("{}", asyncSourceRecord);
         sourceRecordProducer.produceSync(key, asyncSourceRecord);
      } catch (NullPointerException ex) {
         LOGGER.error(ex.getLocalizedMessage(), ex);
      }
   }

   private void apacheReadCSV(final String fileName)
         throws InterruptedException, ExecutionException {
      try {
         final var tuple3 = parseFileName(fileName);
         final var reader = Files.newBufferedReader(Paths.get(fileName));
         final var dtf = DateTimeFormatter.ofPattern("uuuu/MM/dd HH:mm:ss");
         final var now = LocalDateTime.now();
         final var stanDate = dtf.format(now);
         final var uuid = UUID.randomUUID().toString();
         final var batchMetaData = new BatchMetaData(BatchMetaData.FileType.CSV,
                                                     LocalDateTime.now().toString(),
                                                     fileName,
                                                     null,
                                                     null,
                                                     null,
                                                     tuple3 != null
                                                           ? tuple3._3()
                                                           : null);
         final var csvParser = CSVFormat
               .DEFAULT
               .builder()
               .setHeader()
               .setSkipHeaderRecord(false)
               .setIgnoreEmptyLines(true)
               .setNullString(null)
               .build()
               .parse(reader);
         int index = 0;
         sendToKafka(uuid,
                     new AsyncSourceRecord(AsyncSourceRecord.RecordType.BATCH_START,
                                           batchMetaData,
                                           null));
         for (CSVRecord csvRecord : csvParser) {
            // final var clinicalData = csvRecord.get(6);
            final var dwhId = dwh.insertClinicalData(csvRecord.get(0), csvRecord.get(1), csvRecord.get(2), csvRecord.get(3));
            // TODO Use patientpk,sitecode combination
            sendToKafka(uuid,
                        new AsyncSourceRecord(AsyncSourceRecord.RecordType.BATCH_RECORD,
                                              batchMetaData,
                                              new CustomSourceRecord(
                                                    String.format("%s:%07d", stanDate, ++index),
                                                    new SourceId(null, csvRecord.get(1), csvRecord.get(0)),
                                                    null,
                                                    dwhId,
                                                    csvRecord.get(0),
                                                    csvRecord.get(1),
                                                    csvRecord.get(2),
                                                    csvRecord.get(3)
                                                    )));
         }
         sendToKafka(uuid,
                     new AsyncSourceRecord(AsyncSourceRecord.RecordType.BATCH_END,
                                           batchMetaData,
                                           null));
      } catch (IOException ex) {
         LOGGER.error(ex.getLocalizedMessage(), ex);
      }
   }

   private void handleEvent(final WatchEvent<?> event)
         throws InterruptedException, ExecutionException {
      WatchEvent.Kind<?> kind = event.kind();
      LOGGER.info("EVENT: {}", kind);
      if (ENTRY_CREATE.equals(kind)) {
         WatchEvent<Path> ev = cast(event);
         Path filename = ev.context();
         String name = filename.toString();
         LOGGER.info("A new file {} was created", filename);
         if (name.endsWith(".csv")) {
            LOGGER.info("Process CSV file: {}", filename);
            apacheReadCSV("csv/" + filename);
         }
      } else if (ENTRY_MODIFY.equals(kind)) {
         LOGGER.info("EVENT:{}", kind);
      } else if (ENTRY_DELETE.equals(kind)) {
         LOGGER.info("EVENT: {}", kind);
      }
   }

   private Serializer<String> keySerializer() {
      return new StringSerializer();
   }

   private Serializer<AsyncSourceRecord> valueSerializer() {
      return new JsonPojoSerializer<>();
   }

   private void run() throws InterruptedException, ExecutionException, IOException {
      LOGGER.info("KAFKA: {} {} {}",
                  AppConfig.KAFKA_BOOTSTRAP_SERVERS,
                  AppConfig.KAFKA_APPLICATION_ID,
                  AppConfig.KAFKA_CLIENT_ID);
      dwh = new DWH();
      sourceRecordProducer = new MyKafkaProducer<>(AppConfig.KAFKA_BOOTSTRAP_SERVERS,
                                                   GlobalConstants.TOPIC_PATIENT_ASYNC_ETL,
                                                   keySerializer(), valueSerializer(),
                                                   AppConfig.KAFKA_CLIENT_ID);
      final BackPatchStream backPatchStream = BackPatchStream.create();
      backPatchStream.open();
      try (WatchService watcher = FileSystems.getDefault().newWatchService()) {
         Path csvDir = Paths.get("/app/csv");
         csvDir.register(watcher, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE);
         while (true) {
            WatchKey key = watcher.take();
            for (WatchEvent<?> event : key.pollEvents()) {
               handleEvent(event);
            }
            key.reset();
         }
      }
   }

   private String getEncodedMF(
         final String value,
         final OperationType algorithmType) {
      return switch (algorithmType) {
         case OPERATION_TYPE_METAPHONE -> (new Metaphone()).metaphone(value);
         case OPERATION_TYPE_DOUBLE_METAPHONE -> (new DoubleMetaphone()).doubleMetaphone(value);
         case OPERATION_TYPE_SOUNDEX -> (new Soundex()).encode(value);
         case OPERATION_TYPE_REFINED_SOUNDEX -> (new RefinedSoundex()).encode(value);
      };
   }

   enum OperationType {
      OPERATION_TYPE_METAPHONE,
      OPERATION_TYPE_DOUBLE_METAPHONE,
      OPERATION_TYPE_SOUNDEX,
      OPERATION_TYPE_REFINED_SOUNDEX
   }

}
