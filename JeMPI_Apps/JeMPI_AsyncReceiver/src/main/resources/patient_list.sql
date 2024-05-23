with
    ct_patient_source
        as
        (
            select
                distinct
                patients.PatientID as CCCNumber,
                patients.PatientPK,
                patients.SiteCode,
                case
                    when Gender = 'F' then 'Female'
                    when Gender = 'M' then 'Male'
                    when Gender = '' then NULL
                    else Gender
                    end as Gender,
                cast(DOB as date) as DOB,
                case
                    when NUPI = '' then NULL
                    else  NUPI
                    end as NUPI,
                Pkv,
                'C&T' as Docket
            from
                ODS.dbo.CT_Patient as patients
        )

        ,
    hts_patient_source
        as
        (
            select
                distinct
                PatientPK,
                SiteCode,
                case
                    when Gender = 'F' then 'Female'
                    when Gender = 'M' then 'Male'
                    when Gender = '' then NULL
                    else Gender
                    end as Gender,
                cast(DOB as date) as DOB,
                case
                    when NUPI = '' then NULL
                    else  NUPI
                    end as NUPI,
                PKV,
                'HTS' as Docket
            from ODS.dbo.HTS_clients as clients
        )

        ,
    prep_patient_source
        as
        (
            select
                distinct
                PatientPk,
                PrepNumber,
                SiteCode,
                Sex AS Gender,
                cast(DateofBirth as date) as DOB,
                'PrEP' as Docket
            from ODS.dbo.PrEP_Patient
        )

        ,
    mnch_patient_source
        as
        (
            select
                distinct
                PatientPk,
                SiteCode,
                Gender,
                cast(DOB as date) as DOB,
                case
                    when NUPI = '' then NULL
                    else  NUPI
                    end as NUPI,
                Pkv,
                'MNCH' as Docket
            from ODS.dbo.MNCH_Patient
        )

        ,
    combined_data_ct_hts
        as
        (
            select
                coalesce(ct_patient_source.PatientPK, hts_patient_source.PatientPK) as PatientPK,
                coalesce(ct_patient_source.SiteCode, hts_patient_source.SiteCode) as SiteCode,
                ct_patient_source.CCCNumber,
                coalesce(ct_patient_source.NUPI, hts_patient_source.NUPI) as NUPI,
                coalesce(ct_patient_source.DOB, hts_patient_source.DOB) as DOB,
                coalesce(ct_patient_source.Gender, hts_patient_source.Gender) as Gender,
                coalesce(ct_patient_source.PKV, hts_patient_source.PKV) as PKV,
                iif(ct_patient_source.Docket is not null and hts_patient_source.Docket is not null,
                    CONCAT_WS('|', ct_patient_source.Docket, hts_patient_source.Docket),
                    coalesce(ct_patient_source.Docket, hts_patient_source.Docket)) as Docket
            from ct_patient_source full join hts_patient_source on  hts_patient_source.PatientPK = ct_patient_source.PatientPK
                and ct_patient_source.SiteCode = hts_patient_source.SiteCode
        ),

    combined_data_ct_hts_prep
        as
        (
            select
                coalesce(combined_data_ct_hts.PatientPK, prep_patient_source.PatientPK) as PatientPK,
                coalesce(combined_data_ct_hts.SiteCode, prep_patient_source.SiteCode) as SiteCode,
                combined_data_ct_hts.CCCNumber,
                combined_data_ct_hts.NUPI as NUPI,
                coalesce(combined_data_ct_hts.DOB, prep_patient_source.DOB) as DOB,
                coalesce(combined_data_ct_hts.Gender, prep_patient_source.Gender) as Gender,
                combined_data_ct_hts.PKV,
                iif(combined_data_ct_hts.Docket is not null and prep_patient_source.Docket is not null,
                    CONCAT_WS('|', combined_data_ct_hts.Docket, prep_patient_source.Docket),
                    coalesce(combined_data_ct_hts.Docket, prep_patient_source.Docket)) as Docket
            from combined_data_ct_hts
                     full join prep_patient_source on combined_data_ct_hts.PatientPK = prep_patient_source.PatientPK
                and prep_patient_source.SiteCode = combined_data_ct_hts.SiteCode
        ),

    combined_data_ct_hts_prep_mnch
        as
        (
            select
                coalesce(combined_data_ct_hts_prep.PatientPK, mnch_patient_source.PatientPK) as PatientPK,
                coalesce(combined_data_ct_hts_prep.SiteCode, mnch_patient_source.SiteCode) as SiteCode,
                combined_data_ct_hts_prep.CCCNumber,
                coalesce(combined_data_ct_hts_prep.NUPI, mnch_patient_source.NUPI) as NUPI,
                coalesce(combined_data_ct_hts_prep.DOB, mnch_patient_source.DOB) as DOB,
                coalesce(combined_data_ct_hts_prep.Gender, mnch_patient_source.Gender) as Gender,
                coalesce(combined_data_ct_hts_prep.PKV, mnch_patient_source.PKV) as PKV,
                iif(combined_data_ct_hts_prep.Docket is not null and mnch_patient_source.Docket is not null,
                    CONCAT_WS('|', combined_data_ct_hts_prep.Docket, mnch_patient_source.Docket),
                    coalesce(combined_data_ct_hts_prep.Docket, mnch_patient_source.Docket)) as Docket
            from combined_data_ct_hts_prep full join mnch_patient_source on combined_data_ct_hts_prep.PatientPK = mnch_patient_source.PatientPk
                and combined_data_ct_hts_prep.SiteCode = mnch_patient_source.SiteCode
        )
        ,
    verified_list
        as
        (
            select PKV, Gender, DOB, NUPI, SiteCode, PatientPK, CCCNumber, docket
            from combined_data_ct_hts_prep_mnch
            WHERE NUPI IS NOT NULL
        )

        ,
    new_patient_list
        as
        (
            select vl.*
            from verified_list vl
                     left join ODS.dbo.MPI_MatchingOutput cl on cl.patient_pk = vl.PatientPK and cl.site_code = vl.SiteCode
            where cl.patient_pk is null
        )
select *
from new_patient_list