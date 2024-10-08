package org.jembi.jempi.shared.models;

public final class GlobalConstants {

   public static final String TOPIC_INTERACTION_ETL = "JeMPI-interaction-etl";
   public static final String TOPIC_INTERACTION_CONTROLLER = "JeMPI-interaction-controller";
   public static final String TOPIC_INTERACTION_PROCESSOR_CONTROLLER = "JeMPI-interaction-processor-controller";
   public static final String TOPIC_INTERACTION_EM = "JeMPI-interaction-em";
   public static final String TOPIC_INTERACTION_LINKER = "JeMPI-interaction-linker";
   public static final String TOPIC_MU_CONTROLLER = "JeMPI-mu-controller";
   public static final String TOPIC_MU_LINKER = "JeMPI-mu-linker";
   public static final String TOPIC_AUDIT_TRAIL = "JeMPI-audit-trail";
   public static final String TOPIC_NOTIFICATIONS = "JeMPI-notifications";
   public static final String TOPIC_BACK_PATCH_DWH = "JeMPI-back-patch-dwh";
   public static final String TOPIC_SYNC_PATIENTS_DWH = "JeMPI-sync-patients-dwh";
   public static final String TOPIC_MATCH_DATA_DWH = "JeMPI-match-data-dwh";
   public static final String TOPIC_VALIDATION_DATA_DWH = "JeMPI-failed-validation-data-dwh";


   public static final String PSQL_TABLE_AUDIT_TRAIL = "audit_trail";


   /*
    *
    * HTTP SEGMENTS
    *
    */
   public static final String SEGMENT_COUNT_INTERACTIONS = "count-interactions";
   public static final String SEGMENT_COUNT_GOLDEN_RECORDS = "count-golden-records";
   public static final String SEGMENT_COUNT_RECORDS = "count-records";

   public static final String SEGMENT_GET_GIDS_ALL = "gids-all";
   public static final String SEGMENT_GET_GIDS_PAGED = "gids-paged";
   public static final String SEGMENT_GET_INTERACTION = "interaction";
   public static final String SEGMENT_GET_EXPANDED_GOLDEN_RECORD = "expanded-golden-record";
   public static final String SEGMENT_GET_EXPANDED_GOLDEN_RECORDS_USING_PARAMETER_LIST = "expanded-golden-records";
   public static final String SEGMENT_GET_EXPANDED_GOLDEN_RECORDS_USING_CSV = "expanded-golden-records-csv";
   public static final String SEGMENT_GET_EXPANDED_INTERACTIONS_USING_CSV = "expanded-interactions-csv";
   public static final String SEGMENT_GET_GOLDEN_RECORD_AUDIT_TRAIL = "golden-record-audit-trail";
   public static final String SEGMENT_GET_INTERACTION_AUDIT_TRAIL = "interaction-audit-trail";
   public static final String SEGMENT_GET_FIELDS_CONFIG = "config";
   public static final String SEGMENT_GET_LINKED_RECORDS = "LinkedRecords";
   public static final String SEGMENT_GET_NOTIFICATIONS = "MatchesForReview";

   public static final String SEGMENT_PATCH_GOLDEN_RECORD = "golden-record";
   public static final String SEGMENT_POST_IID_GID_LINK = "Link";
   public static final String SEGMENT_POST_IID_NEW_GID_LINK = "Unlink";

   public static final String SEGMENT_POST_UPDATE_NOTIFICATION = "NotificationRequest";
   public static final String SEGMENT_POST_SIMPLE_SEARCH = "search";
   public static final String SEGMENT_POST_CUSTOM_SEARCH = "custom-search";
   public static final String SEGMENT_POST_UPLOAD_CSV_FILE = "Upload";
   public static final String SEGMENT_POST_FILTER_GIDS = "filter-gids";
   public static final String SEGMENT_POST_FILTER_GIDS_WITH_INTERACTION_COUNT = "filter-gids-interaction";

   public static final String SEGMENT_PROXY_POST_CR_REGISTER = "cr-register";
   public static final String SEGMENT_PROXY_POST_CR_FIND = "cr-find";
   public static final String SEGMENT_PROXY_POST_CR_CANDIDATES = "cr-candidates";
   public static final String SEGMENT_PROXY_PATCH_CR_UPDATE_FIELDS = "cr-update-fields";


   public static final String SEGMENT_PROXY_GET_CANDIDATES_WITH_SCORES = "candidate-golden-records";
   public static final String SEGMENT_PROXY_POST_CALCULATE_SCORES = "calculate-scores";

   public static final String SEGMENT_PROXY_GET_DASHBOARD_DATA = "dashboard-data";

   public static final String SEGMENT_PROXY_ON_NOTIFICATION_RESOLUTION = "on-notification-resolution";
   public static final String SEGMENT_PROXY_POST_LINK_INTERACTION = "link-interaction";
   public static final String SEGMENT_PROXY_POST_LINK_INTERACTION_TO_GID = "link-interaction-to-gid";

   public static final String SEGMENT_SCHEMA_RECREATE = "recreate-schema";

   public static final String SEGMENT_SYNC_PATIENT_LIST = "sync-patients";

   public static final String SEGMENT_VALIDATE_OAUTH = "authenticate";
   //                         SEGMENT_VALIDATE_OAUTH: '/authenticate',
   public static final String SEGMENT_LOGOUT = "logout";
   //                         SEGMENT_LOGOUT: '/logout',
   public static final String SEGMENT_CURRENT_USER = "current-user";
   //                         SEGMENT_CURRENT_USER: '/current-user',


   public static final String DEFAULT_LINKER_GLOBAL_STORE_NAME = "linker";
   private GlobalConstants() {
   }

}
