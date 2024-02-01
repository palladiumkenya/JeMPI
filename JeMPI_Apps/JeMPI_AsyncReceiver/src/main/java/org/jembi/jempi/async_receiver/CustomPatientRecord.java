package org.jembi.jempi.async_receiver;

import java.sql.Date;

public record CustomPatientRecord(
        String cccNumber,
        String pkv,
        String docket,
        String gender,
        Date dob,
        String nupi,
        String siteCode,
        String patientPk
) {
}
