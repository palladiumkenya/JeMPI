package org.jembi.jempi.etl;

import org.jembi.jempi.shared.models.CustomDemographicData;
import org.jembi.jempi.shared.models.CustomUniqueInteractionData;
import scala.Tuple2;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class ETLUtil {
    private ETLUtil() {
    }

    static CustomDemographicData addPhoneticsToDemographicData(final CustomDemographicData demographicData, final CustomUniqueInteractionData uniqueInteractionData) {
        Tuple2<String, String> phoneticNames =  parsePkv(uniqueInteractionData.pkv());
        String givenNameSoundex = phoneticNames == null ? "" : phoneticNames._1();
        String familyNameDoubleMetaphone = phoneticNames == null ? "" : phoneticNames._2();
        return new CustomDemographicData(
                givenNameSoundex,
                familyNameDoubleMetaphone,
                demographicData.getGender(),
                demographicData.getDob(),
                demographicData.getNupi(),
                demographicData.getCccNumber().replaceAll("([\\\\])", "/"),
                demographicData.getDocket()
        );
    }

//    static CustomUniqueInteractionData cleanUniqueInteractionData(final CustomUniqueInteractionData uniqueInteractionData) {
//        return new CustomUniqueInteractionData(
//                uniqueInteractionData.auxDateCreated(),
//                uniqueInteractionData.auxId(),
//                uniqueInteractionData.pkv(),
//                uniqueInteractionData.auxDwhId()
//        );
//    }


    static Tuple2<String, String> parsePkv(final String pkv) {
        if (pkv != null && !pkv.isEmpty()) {
            final String regex = "^(?<gender>[M|F])(?<pgn>[A-Z]\\d+)(?<pfn>[A-Z]+)(?<dob>\\d\\d\\d\\d)$";
            final Pattern pattern = Pattern.compile(regex);
            final Matcher matcher = pattern.matcher(pkv);
            if (matcher.find()) {
                final var phoneticGivenName = matcher.group("pgn");
                final var phoneticFamilyName = matcher.group("pfn");
                return new Tuple2<>(phoneticGivenName, phoneticFamilyName);
            }
        }
        return null;
    }
}
