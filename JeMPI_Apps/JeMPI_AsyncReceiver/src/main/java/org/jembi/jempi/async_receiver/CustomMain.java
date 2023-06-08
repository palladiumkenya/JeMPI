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
import scala.Tuple2;
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
   private static final int LIVE_PKV_IDX = 0;
   private static final int LIVE_GENDER_IDX = 1;
   private static final int LIVE_DOB_IDX = 2;
   private static final int LIVE_NUPI_IDX = 3;
   private static final int LIVE_SITE_CODE_IDX = 4;
   private static final int LIVE_PATIENT_PK_IDX = 5;
   private static final int LIVE_CCC_NUMBER_IDX = 6;

   private static final int TEST_AUX_ID_IDX = 0;
   private static final int TEST_GIVEN_NAME_IDX = 1;
   private static final int TEST_FAMILY_NAME_IDX = 2;
   private static final int TEST_GENDER_IDX = 3;
   private static final int TEST_DOB_IDX = 4;
   private static final int TEST_NUPI_IDX = 5;
   private static final int TEST_CCC_NUMBER_IDX = 6;
   private static final int TEST_CLINICAL_IDX = 7;
   

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

   private static Tuple2<String, String> parsePkv(final String pkv) {
      final String regex = "^(?<gender>[M|F])(?<pgn>[A-Z]\\d+)(?<pfn>[A-Z]+)(?<dob>\\d\\d\\d\\d)$";
      final Pattern pattern = Pattern.compile(regex);
      final Matcher matcher = pattern.matcher(pkv);
      if (matcher.find()) {
         final var phoneticGivenName = matcher.group("pgn");
         final var phoneticFamilyName = matcher.group("pfn");
         return new Tuple2<>(phoneticGivenName, phoneticFamilyName);
      }
      return null;
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

   private CustomSourceRecord parseFakeRecord(
         final String stan,
         final String dwhId,
         final CSVRecord csvRecord) {
      LOGGER.debug("{} {} {}", stan, dwhId, csvRecord.stream().toList());

      return new CustomSourceRecord(
            stan,
            parseSourceId(csvRecord.get(TEST_CLINICAL_IDX)),
            csvRecord.get(TEST_AUX_ID_IDX),
            dwhId,
//            getEncodedMF(csvRecord.get(TEST_GIVEN_NAME_IDX), OperationType.OPERATION_TYPE_SOUNDEX),
//            getEncodedMF(csvRecord.get(TEST_FAMILY_NAME_IDX), OperationType.OPERATION_TYPE_SOUNDEX),
            csvRecord.get(TEST_GIVEN_NAME_IDX),
            csvRecord.get(TEST_FAMILY_NAME_IDX),
            csvRecord.get(TEST_GENDER_IDX),
            csvRecord.get(TEST_DOB_IDX),
            csvRecord.get(TEST_NUPI_IDX));
   }

   private CustomSourceRecord parseLiveRecord(
         final String stan,
         final String dwhId,
         final CSVRecord csvRecord) {
      final var phoneticTuple = parsePkv(csvRecord.get(LIVE_PKV_IDX));
      final var dob = csvRecord.get(LIVE_DOB_IDX);
      final var nupi = csvRecord.get(LIVE_NUPI_IDX) == null || csvRecord.get(LIVE_NUPI_IDX).isEmpty() ? null : csvRecord.get(LIVE_NUPI_IDX);
      final var givenNameSoundex = phoneticTuple == null
            ? (dob != null ? getEncodedMF(dob, OperationType.OPERATION_TYPE_SOUNDEX) : null)
            : phoneticTuple._1();
      final var familyNameDoubleMetaphone = phoneticTuple == null
            ? (dob != null ? getEncodedMF(dob, OperationType.OPERATION_TYPE_DOUBLE_METAPHONE) : null)
            : phoneticTuple._2();
      return new CustomSourceRecord(
            stan,
            new SourceId(null, csvRecord.get(LIVE_SITE_CODE_IDX), csvRecord.get(LIVE_PATIENT_PK_IDX)),
            null,
            dwhId,
            givenNameSoundex,
            familyNameDoubleMetaphone,
            csvRecord.get(LIVE_GENDER_IDX),
            csvRecord.get(LIVE_DOB_IDX),
            nupi);

   }

   private String dbInsertFakeData(final CSVRecord csvRecord) {
      final var sourceId = parseSourceId(csvRecord.get(TEST_CLINICAL_IDX));
      final var pkv = String.format("%s%s",
                                    csvRecord.get(TEST_GIVEN_NAME_IDX),
                                    csvRecord.get(TEST_FAMILY_NAME_IDX));
      return dwh.insertClinicalData(pkv,
            csvRecord.get(TEST_GENDER_IDX),
            csvRecord.get(TEST_DOB_IDX),
            csvRecord.get(TEST_NUPI_IDX),
            sourceId.facility(),
            sourceId.patient(),
            csvRecord.get(TEST_CCC_NUMBER_IDX));
   }

   private String dbInsertLiveData(final CSVRecord csvRecord) {
      return dwh.insertClinicalData(csvRecord.get(LIVE_PKV_IDX),
            csvRecord.get(LIVE_GENDER_IDX),
            csvRecord.get(LIVE_DOB_IDX),
            csvRecord.get(LIVE_NUPI_IDX),
            csvRecord.get(LIVE_SITE_CODE_IDX),
            csvRecord.get(LIVE_PATIENT_PK_IDX),
            csvRecord.get(LIVE_CCC_NUMBER_IDX));
   }

   private void sendToKafka(
         final String key,
         final AsyncSourceRecord asyncSourceRecord)
         throws InterruptedException, ExecutionException {
      try {
//         LOGGER.debug("{}", asyncSourceRecord);
         sourceRecordProducer.produceSync(key, asyncSourceRecord);
      } catch (NullPointerException ex) {
         LOGGER.error(ex.getLocalizedMessage(), ex);
      }
   }

   private void apacheReadCSV(final String fileName)
         throws InterruptedException, ExecutionException {
      try {
         // final var tuple3 = parseFileName(fileName);
         final var threshold = AppConfig.INPUT_DEFAULT_THRESHOLD;
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
                                                     threshold);
         final var csvParser = CSVFormat
               .DEFAULT
               .builder()
               .setHeader()
               .setSkipHeaderRecord(true)
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
            // final var pkv = String.format("(%s-%s)-(%s-%s)-%s-%s",
            //                               csvRecord.get(GIVEN_NAME_IDX),
            //                               tuple3 == null
            //                                     ? csvRecord.get(GIVEN_NAME_IDX)
            //                                     : getEncodedMF(csvRecord.get(GIVEN_NAME_IDX),
            //                                                    tuple3._1()),
            //                               csvRecord.get(FAMILY_NAME_IDX),
            //                               tuple3 == null
            //                                     ? csvRecord.get(FAMILY_NAME_IDX)
            //                                     : getEncodedMF(csvRecord.get(FAMILY_NAME_IDX),
            //                                                    tuple3._2()),
            //                               csvRecord.get(GENDER_IDX),
            //                               csvRecord.get(DOB_IDX));
            // LOGGER.debug("pkv: {}", pkv);
            //var dwhId;
            final CustomSourceRecord sourceRecord;
            final String dwhId;
            if (AppConfig.INPUT_ENVIRONMENT.equals("prod")) {
               dwhId = dbInsertLiveData(csvRecord);
               sourceRecord = parseLiveRecord(String.format("%s:%07d", stanDate, ++index), dwhId, csvRecord);
            } else {
               dwhId = dbInsertFakeData(csvRecord);
               sourceRecord = parseFakeRecord(String.format("%s:%07d", stanDate, ++index), dwhId, csvRecord);
            }

//            final var dwhId = dbInsertLiveData(csvRecord);
//            final var sourceRecord = parseLiveRecord(String.format("%s:%07d", stanDate, ++index), dwhId, csvRecord);
//            final var dwhId = dbInsertFakeData(csvRecord);
//            final var sourceRecord = parseFakeRecord(String.format("%s:%07d", stanDate, ++index), dwhId, csvRecord);

//            final var phoneticTuple = parsePkv(csvRecord.get(LIVE_PKV_IDX));
//            final var customSourceRecord = new CustomSourceRecord(
//                  String.format("%s:%07d", stanDate, ++index),
//                  new SourceId(null, csvRecord.get(LIVE_SITE_CODE_IDX), csvRecord.get(LIVE_PATIENT_PK_IDX)),
//                  null,
//                  dwhId,
//                  phoneticTuple != null
//                        ? phoneticTuple._1()
//                        : null,
//                  phoneticTuple != null
//                        ? phoneticTuple._2()
//                        : null,
//                  csvRecord.get(LIVE_GENDER_IDX),
//                  csvRecord.get(LIVE_DOB_IDX),
//                  csvRecord.get(LIVE_NUPI_IDX));

            final var asyncSourceRecord = new AsyncSourceRecord(AsyncSourceRecord.RecordType.BATCH_RECORD,
                                                                batchMetaData,
                                                                sourceRecord);
//            LOGGER.debug("{}", dwhId);
//            LOGGER.debug("{}", sourceRecord);
//            LOGGER.debug("{}", asyncSourceRecord);
            sendToKafka(UUID.randomUUID().toString(), asyncSourceRecord);
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
