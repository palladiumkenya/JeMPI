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
/*
0.55 - SOUNDEX DOUBLE-METAPHONE
[INFO ] 2023-04-09 08:22:31.197 CustomMainStats:151 - Document Count:       5035
[INFO ] 2023-04-09 08:22:31.198 CustomMainStats:152 - Golden Record Count:  1001
[INFO ] 2023-04-09 08:22:31.198 CustomMainStats:153 - Number of Records:    5035,1001
[INFO ] 2023-04-09 08:22:31.198 CustomMainStats:154 - Number if id's:       1001
[INFO ] 2023-04-09 08:22:31.198 CustomMainStats:159 - Golden Records:       1001
[INFO ] 2023-04-09 08:22:31.198 CustomMainStats:160 - Sub List Size:        100
[INFO ] 2023-04-09 08:22:31.198 CustomMainStats:161 - Sub Lists:            10
[INFO ] 2023-04-09 08:22:31.198 CustomMainStats:162 - Final Sub List Size:  1
[INFO ] 2023-04-09 08:22:31.457 CustomMainStats:202 - Golden Records Found: 962
[INFO ] 2023-04-09 08:22:31.457 CustomMainStats:203 - TP:4808  FP:164  FN:63  Precision:0.9670152855993563  Recall:0.9870663108191337  F-score:0.976937925429239

0.60 - SOUNDEX DOUBLE-METAPHONE
[INFO ] 2023-04-09 08:57:10.537 CustomMainStats:151 - Document Count:       5035
[INFO ] 2023-04-09 08:57:10.537 CustomMainStats:152 - Golden Record Count:  1009
[INFO ] 2023-04-09 08:57:10.537 CustomMainStats:153 - Number of Records:    5035,1009
[INFO ] 2023-04-09 08:57:10.537 CustomMainStats:154 - Number if id's:       1009
[INFO ] 2023-04-09 08:57:10.537 CustomMainStats:159 - Golden Records:       1009
[INFO ] 2023-04-09 08:57:10.538 CustomMainStats:160 - Sub List Size:        100
[INFO ] 2023-04-09 08:57:10.538 CustomMainStats:161 - Sub Lists:            10
[INFO ] 2023-04-09 08:57:10.538 CustomMainStats:162 - Final Sub List Size:  9
[INFO ] 2023-04-09 08:57:10.852 CustomMainStats:202 - Golden Records Found: 972
[INFO ] 2023-04-09 08:57:10.852 CustomMainStats:203 - TP:4863  FP:112  FN:60  Precision:0.9774874371859297  Recall:0.987812309567337  F-score:0.9826227520711255

0.63 - SOUNDEX DOUBLE-METAPHONE
[INFO ] 2023-04-09 09:07:01.762 CustomMainStats:151 - Document Count:       5035
[INFO ] 2023-04-09 09:07:01.762 CustomMainStats:152 - Golden Record Count:  1009
[INFO ] 2023-04-09 09:07:01.762 CustomMainStats:153 - Number of Records:    5035,1009
[INFO ] 2023-04-09 09:07:01.762 CustomMainStats:154 - Number if id's:       1009
[INFO ] 2023-04-09 09:07:01.763 CustomMainStats:159 - Golden Records:       1009
[INFO ] 2023-04-09 09:07:01.763 CustomMainStats:160 - Sub List Size:        100
[INFO ] 2023-04-09 09:07:01.763 CustomMainStats:161 - Sub Lists:            10
[INFO ] 2023-04-09 09:07:01.763 CustomMainStats:162 - Final Sub List Size:  9
[INFO ] 2023-04-09 09:07:02.121 CustomMainStats:202 - Golden Records Found: 972
[INFO ] 2023-04-09 09:07:02.122 CustomMainStats:203 - TP:4864  FP:111  FN:60  Precision:0.9776884422110553  Recall:0.9878147847278635  F-score:0.9827255278310942

0.64 - SOUNDEX DOUBLE-METAPHONE
[INFO ] 2023-04-09 09:13:15.950 CustomMainStats:151 - Document Count:       5035
[INFO ] 2023-04-09 09:13:15.950 CustomMainStats:152 - Golden Record Count:  1010
[INFO ] 2023-04-09 09:13:15.951 CustomMainStats:153 - Number of Records:    5035,1010
[INFO ] 2023-04-09 09:13:15.951 CustomMainStats:154 - Number if id's:       1010
[INFO ] 2023-04-09 09:13:15.951 CustomMainStats:159 - Golden Records:       1010
[INFO ] 2023-04-09 09:13:15.951 CustomMainStats:160 - Sub List Size:        100
[INFO ] 2023-04-09 09:13:15.951 CustomMainStats:161 - Sub Lists:            10
[INFO ] 2023-04-09 09:13:15.951 CustomMainStats:162 - Final Sub List Size:  10
[INFO ] 2023-04-09 09:13:16.259 CustomMainStats:202 - Golden Records Found: 973
[INFO ] 2023-04-09 09:13:16.260 CustomMainStats:203 - TP:4866  FP:109  FN:60  Precision:0.9780904522613065  Recall:0.9878197320341048  F-score:0.9829310170689829

0.65 - SOUNDEX DOUBLE-METAPHONE
[INFO ] 2023-04-09 08:52:14.034 CustomMainStats:151 - Document Count:       5035
[INFO ] 2023-04-09 08:52:14.035 CustomMainStats:152 - Golden Record Count:  1023
[INFO ] 2023-04-09 08:52:14.035 CustomMainStats:153 - Number of Records:    5035,1023
[INFO ] 2023-04-09 08:52:14.035 CustomMainStats:154 - Number if id's:       1023
[INFO ] 2023-04-09 08:52:14.035 CustomMainStats:159 - Golden Records:       1023
[INFO ] 2023-04-09 08:52:14.035 CustomMainStats:160 - Sub List Size:        100
[INFO ] 2023-04-09 08:52:14.035 CustomMainStats:161 - Sub Lists:            10
[INFO ] 2023-04-09 08:52:14.035 CustomMainStats:162 - Final Sub List Size:  23
[INFO ] 2023-04-09 08:52:14.284 CustomMainStats:202 - Golden Records Found: 985
[INFO ] 2023-04-09 08:52:14.285 CustomMainStats:203 - TP:4903  FP:71  FN:61  Precision:0.9857257740249297  Recall:0.9877115229653505  F-score:0.986717649426444

0.655 - SOUNDEX DOUBLE-METAPHONE
[INFO ] 2023-04-09 09:23:22.397 CustomMainStats:151 - Document Count:       5035
[INFO ] 2023-04-09 09:23:22.397 CustomMainStats:152 - Golden Record Count:  1023
[INFO ] 2023-04-09 09:23:22.397 CustomMainStats:153 - Number of Records:    5035,1023
[INFO ] 2023-04-09 09:23:22.397 CustomMainStats:154 - Number if id's:       1023
[INFO ] 2023-04-09 09:23:22.397 CustomMainStats:159 - Golden Records:       1023
[INFO ] 2023-04-09 09:23:22.397 CustomMainStats:160 - Sub List Size:        100
[INFO ] 2023-04-09 09:23:22.397 CustomMainStats:161 - Sub Lists:            10
[INFO ] 2023-04-09 09:23:22.397 CustomMainStats:162 - Final Sub List Size:  23
[INFO ] 2023-04-09 09:23:22.738 CustomMainStats:202 - Golden Records Found: 985
[INFO ] 2023-04-09 09:23:22.739 CustomMainStats:203 - TP:4903  FP:71  FN:61  Precision:0.9857257740249297  Recall:0.9877115229653505  F-score:0.986717649426444

0.66 - SOUNDEX DOUBLE-METAPHONE
[INFO ] 2023-04-09 09:18:07.471 CustomMainStats:151 - Document Count:       5035
[INFO ] 2023-04-09 09:18:07.471 CustomMainStats:152 - Golden Record Count:  1023
[INFO ] 2023-04-09 09:18:07.471 CustomMainStats:153 - Number of Records:    5035,1023
[INFO ] 2023-04-09 09:18:07.471 CustomMainStats:154 - Number if id's:       1023
[INFO ] 2023-04-09 09:18:07.471 CustomMainStats:159 - Golden Records:       1023
[INFO ] 2023-04-09 09:18:07.472 CustomMainStats:160 - Sub List Size:        100
[INFO ] 2023-04-09 09:18:07.472 CustomMainStats:161 - Sub Lists:            10
[INFO ] 2023-04-09 09:18:07.472 CustomMainStats:162 - Final Sub List Size:  23
[INFO ] 2023-04-09 09:18:07.807 CustomMainStats:202 - Golden Records Found: 985
[INFO ] 2023-04-09 09:18:07.807 CustomMainStats:203 - TP:4903  FP:71  FN:61  Precision:0.9857257740249297  Recall:0.9877115229653505  F-score:0.986717649426444

0.67 - SOUNDEX DOUBLE-METAPHONE
[INFO ] 2023-04-09 09:02:17.640 CustomMainStats:151 - Document Count:       5035
[INFO ] 2023-04-09 09:02:17.641 CustomMainStats:152 - Golden Record Count:  1253
[INFO ] 2023-04-09 09:02:17.641 CustomMainStats:153 - Number of Records:    5035,1253
[INFO ] 2023-04-09 09:02:17.641 CustomMainStats:154 - Number if id's:       1253
[INFO ] 2023-04-09 09:02:17.641 CustomMainStats:159 - Golden Records:       1253
[INFO ] 2023-04-09 09:02:17.641 CustomMainStats:160 - Sub List Size:        100
[INFO ] 2023-04-09 09:02:17.641 CustomMainStats:161 - Sub Lists:            12
[INFO ] 2023-04-09 09:02:17.641 CustomMainStats:162 - Final Sub List Size:  53
[INFO ] 2023-04-09 09:02:17.989 CustomMainStats:202 - Golden Records Found: 988
[INFO ] 2023-04-09 09:02:17.989 CustomMainStats:203 - TP:4552  FP:46  FN:437  Precision:0.9899956502827316  Recall:0.9124072960513129  F-score:0.9496192761030562

0.70 - SOUNDEX DOUBLE-METAPHONE
[INFO ] 2023-04-09 08:45:56.317 CustomMainStats:151 - Document Count:       5035
[INFO ] 2023-04-09 08:45:56.317 CustomMainStats:152 - Golden Record Count:  1360
[INFO ] 2023-04-09 08:45:56.317 CustomMainStats:153 - Number of Records:    5035,1360
[INFO ] 2023-04-09 08:45:56.317 CustomMainStats:154 - Number if id's:       1360
[INFO ] 2023-04-09 08:45:56.317 CustomMainStats:159 - Golden Records:       1360
[INFO ] 2023-04-09 08:45:56.317 CustomMainStats:160 - Sub List Size:        100
[INFO ] 2023-04-09 08:45:56.317 CustomMainStats:161 - Sub Lists:            13
[INFO ] 2023-04-09 08:45:56.317 CustomMainStats:162 - Final Sub List Size:  60
[INFO ] 2023-04-09 08:45:56.689 CustomMainStats:202 - Golden Records Found: 999
[INFO ] 2023-04-09 08:45:56.690 CustomMainStats:203 - TP:4474  FP:4  FN:557  Precision:0.9991067440821796  Recall:0.8892864241701451  F-score:0.941003260069408

0.60 - SOUNDEX SOUNDEX
[INFO ] 2023-04-09 14:56:17.078 CustomMainStats:151 - Document Count:       5035
[INFO ] 2023-04-09 14:56:17.078 CustomMainStats:152 - Golden Record Count:  973
[INFO ] 2023-04-09 14:56:17.078 CustomMainStats:153 - Number of Records:    5035,973
[INFO ] 2023-04-09 14:56:17.078 CustomMainStats:154 - Number if id's:       973
[INFO ] 2023-04-09 14:56:17.079 CustomMainStats:159 - Golden Records:       973
[INFO ] 2023-04-09 14:56:17.079 CustomMainStats:160 - Sub List Size:        100
[INFO ] 2023-04-09 14:56:17.079 CustomMainStats:161 - Sub Lists:            9
[INFO ] 2023-04-09 14:56:17.079 CustomMainStats:162 - Final Sub List Size:  73
[INFO ] 2023-04-09 14:56:17.366 CustomMainStats:202 - Golden Records Found: 968
[INFO ] 2023-04-09 14:56:17.367 CustomMainStats:203 - TP:4904  FP:123  FN:8  Precision:0.9755321265168092  Recall:0.998371335504886  F-score:0.9868195995572996

0.64 - SOUNDEX SOUNDEX
[INFO ] 2023-04-09 14:58:41.108 CustomMainStats:151 - Document Count:       5035
[INFO ] 2023-04-09 14:58:41.108 CustomMainStats:152 - Golden Record Count:  974
[INFO ] 2023-04-09 14:58:41.108 CustomMainStats:153 - Number of Records:    5035,974
[INFO ] 2023-04-09 14:58:41.108 CustomMainStats:154 - Number if id's:       974
[INFO ] 2023-04-09 14:58:41.108 CustomMainStats:159 - Golden Records:       974
[INFO ] 2023-04-09 14:58:41.108 CustomMainStats:160 - Sub List Size:        100
[INFO ] 2023-04-09 14:58:41.108 CustomMainStats:161 - Sub Lists:            9
[INFO ] 2023-04-09 14:58:41.109 CustomMainStats:162 - Final Sub List Size:  74
[INFO ] 2023-04-09 14:58:41.401 CustomMainStats:202 - Golden Records Found: 969
[INFO ] 2023-04-09 14:58:41.401 CustomMainStats:203 - TP:4906  FP:121  FN:8  Precision:0.975929978118162  Recall:0.9983719983719984  F-score:0.9870234382858868

0.65 - SOUNDEX SOUNDEX
[INFO ] 2023-04-09 09:58:43.196 CustomMainStats:151 - Document Count:       5035
[INFO ] 2023-04-09 09:58:43.196 CustomMainStats:152 - Golden Record Count:  987
[INFO ] 2023-04-09 09:58:43.196 CustomMainStats:153 - Number of Records:    5035,987
[INFO ] 2023-04-09 09:58:43.197 CustomMainStats:154 - Number if id's:       987
[INFO ] 2023-04-09 09:58:43.197 CustomMainStats:159 - Golden Records:       987
[INFO ] 2023-04-09 09:58:43.197 CustomMainStats:160 - Sub List Size:        100
[INFO ] 2023-04-09 09:58:43.197 CustomMainStats:161 - Sub Lists:            9
[INFO ] 2023-04-09 09:58:43.197 CustomMainStats:162 - Final Sub List Size:  87
[INFO ] 2023-04-09 09:58:43.489 CustomMainStats:202 - Golden Records Found: 983
[INFO ] 2023-04-09 09:58:43.489 CustomMainStats:203 - TP:4947  FP:81  FN:7  Precision:0.983890214797136  Recall:0.9985870004037142  F-score:0.9911841314365858

0.66 - SOUNDEX SOUNDEX
[INFO ] 2023-04-09 15:01:15.973 CustomMainStats:151 - Document Count:       5035
[INFO ] 2023-04-09 15:01:15.973 CustomMainStats:152 - Golden Record Count:  987
[INFO ] 2023-04-09 15:01:15.973 CustomMainStats:153 - Number of Records:    5035,987
[INFO ] 2023-04-09 15:01:15.973 CustomMainStats:154 - Number if id's:       987
[INFO ] 2023-04-09 15:01:15.973 CustomMainStats:159 - Golden Records:       987
[INFO ] 2023-04-09 15:01:15.973 CustomMainStats:160 - Sub List Size:        100
[INFO ] 2023-04-09 15:01:15.974 CustomMainStats:161 - Sub Lists:            9
[INFO ] 2023-04-09 15:01:15.974 CustomMainStats:162 - Final Sub List Size:  87
[INFO ] 2023-04-09 15:01:16.268 CustomMainStats:202 - Golden Records Found: 983
[INFO ] 2023-04-09 15:01:16.268 CustomMainStats:203 - TP:4947  FP:81  FN:7  Precision:0.983890214797136  Recall:0.9985870004037142  F-score:0.9911841314365858

0.70 - SOUNDEX SOUNDEX
[INFO ] 2023-04-09 14:49:54.414 CustomMainStats:151 - Document Count:       5035
[INFO ] 2023-04-09 14:49:54.414 CustomMainStats:152 - Golden Record Count:  1168
[INFO ] 2023-04-09 14:49:54.415 CustomMainStats:153 - Number of Records:    5035,1168
[INFO ] 2023-04-09 14:49:54.415 CustomMainStats:154 - Number if id's:       1168
[INFO ] 2023-04-09 14:49:54.415 CustomMainStats:159 - Golden Records:       1168
[INFO ] 2023-04-09 14:49:54.415 CustomMainStats:160 - Sub List Size:        100
[INFO ] 2023-04-09 14:49:54.415 CustomMainStats:161 - Sub Lists:            11
[INFO ] 2023-04-09 14:49:54.415 CustomMainStats:162 - Final Sub List Size:  68
[INFO ] 2023-04-09 14:49:54.780 CustomMainStats:202 - Golden Records Found: 999
[INFO ] 2023-04-09 14:49:54.780 CustomMainStats:203 - TP:4798  FP:3  FN:234  Precision:0.9993751301812123  Recall:0.9534976152623211  F-score:0.9758974880504425

0.55 - NAMES
[INFO ] 2023-04-09 08:29:41.785 CustomMainStats:151 - Document Count:       5035
[INFO ] 2023-04-09 08:29:41.785 CustomMainStats:152 - Golden Record Count:  1082
[INFO ] 2023-04-09 08:29:41.786 CustomMainStats:153 - Number of Records:    5035,1082
[INFO ] 2023-04-09 08:29:41.786 CustomMainStats:154 - Number if id's:       1082
[INFO ] 2023-04-09 08:29:41.786 CustomMainStats:159 - Golden Records:       1082
[INFO ] 2023-04-09 08:29:41.786 CustomMainStats:160 - Sub List Size:        100
[INFO ] 2023-04-09 08:29:41.786 CustomMainStats:161 - Sub Lists:            10
[INFO ] 2023-04-09 08:29:41.786 CustomMainStats:162 - Final Sub List Size:  82
[INFO ] 2023-04-09 08:29:42.045 CustomMainStats:202 - Golden Records Found: 974
[INFO ] 2023-04-09 08:29:42.046 CustomMainStats:203 - TP:4733  FP:112  FN:190  Precision:0.9768833849329205  Recall:0.9614056469632338  F-score:0.9690827190827191
*/
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
            final var clinicalData = csvRecord.get(6);
            final var dwhId = dwh.insertClinicalData(String.format("%s - %s", csvRecord.get(0), clinicalData));
            sendToKafka(uuid,
                        new AsyncSourceRecord(AsyncSourceRecord.RecordType.BATCH_RECORD,
                                              batchMetaData,
                                              new CustomSourceRecord(
                                                    String.format("%s:%07d", stanDate, ++index),
                                                    parseSourceId(csvRecord.get(6)),
                                                    csvRecord.get(0),
                                                    dwhId,
                                                    tuple3 == null
                                                          ? csvRecord.get(1)
                                                          : getEncodedMF(csvRecord.get(1), tuple3._1()),
                                                    tuple3 == null
                                                          ? csvRecord.get(2)
                                                          : getEncodedMF(csvRecord.get(2), tuple3._2()),
                                                    csvRecord.get(3),
                                                    csvRecord.get(4),
                                                    csvRecord.get(5))));
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
