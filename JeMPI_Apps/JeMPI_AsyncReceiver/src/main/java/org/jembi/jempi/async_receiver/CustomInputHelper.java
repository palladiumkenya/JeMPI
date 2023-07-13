package org.jembi.jempi.async_receiver;

import scala.Tuple2;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class CustomInputHelper {

   private CustomInputHelper() {
   }

   public static Tuple2<String, String> parsePkv(final String pkv) {
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

}
