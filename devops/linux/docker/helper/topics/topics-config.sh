#!/bin/bash

TOPIC_INTERACTION_ASYNC_ETL="JeMPI-async-etl"
TOPIC_INTERACTION_CONTROLLER="JeMPI-interaction-controller"
TOPIC_INTERACTION_EM="JeMPI-interaction-em"
TOPIC_INTERACTION_LINKER="JeMPI-interaction-linker"
TOPIC_MU_LINKER="JeMPI-mu-linker"
TOPIC_AUDIT_TRAIL="JeMPI-audit-trail"
TOPIC_NOTIFICATIONS="JeMPI-notifications"
TOPIC_BACK_PATCH_DWH="JeMPI-back-patch-dwh"
TOPIC_SYNC_PATIENTS_DWH="JeMPI-sync-patients-dwh"

declare -a TOPICS=(
  $TOPIC_INTERACTION_ASYNC_ETL
  $TOPIC_INTERACTION_CONTROLLER
  $TOPIC_INTERACTION_EM
  $TOPIC_INTERACTION_LINKER
  $TOPIC_MU_LINKER
  $TOPIC_AUDIT_TRAIL
  $TOPIC_NOTIFICATIONS
  $TOPIC_BACK_PATCH_DWH
  $TOPIC_SYNC_PATIENTS_DWH
)

declare -A PARTITIONS
PARTITIONS[$TOPIC_INTERACTION_ASYNC_ETL]=1
PARTITIONS[$TOPIC_INTERACTION_CONTROLLER]=1
PARTITIONS[$TOPIC_INTERACTION_EM]=1
PARTITIONS[$TOPIC_INTERACTION_LINKER]=1
PARTITIONS[$TOPIC_MU_LINKER]=1
PARTITIONS[$TOPIC_AUDIT_TRAIL]=1
PARTITIONS[$TOPIC_NOTIFICATIONS]=1
PARTITIONS[$TOPIC_BACK_PATCH_DWH]=1
PARTITIONS[$TOPIC_SYNC_PATIENTS_DWH]=1

declare -A REPLICATION
REPLICATION[$TOPIC_INTERACTION_ASYNC_ETL]=1
REPLICATION[$TOPIC_INTERACTION_CONTROLLER]=1
REPLICATION[$TOPIC_INTERACTION_EM]=1
REPLICATION[$TOPIC_INTERACTION_LINKER]=1
REPLICATION[$TOPIC_MU_LINKER]=1
REPLICATION[$TOPIC_AUDIT_TRAIL]=1
REPLICATION[$TOPIC_NOTIFICATIONS]=1
REPLICATION[$TOPIC_BACK_PATCH_DWH]=1
REPLICATION[$TOPIC_SYNC_PATIENTS_DWH]=1

declare -A RETENTION_MS
RETENTION_MS[$TOPIC_INTERACTION_ASYNC_ETL]=`echo "30*24*60*60*1000" | bc`
RETENTION_MS[$TOPIC_INTERACTION_CONTROLLER]=`echo "30*24*60*60*1000" | bc`
RETENTION_MS[$TOPIC_INTERACTION_EM]=`echo "30*24*60*60*1000" | bc`
RETENTION_MS[$TOPIC_INTERACTION_LINKER]=`echo "30*24*60*60*1000" | bc`
RETENTION_MS[$TOPIC_MU_LINKER]=`echo "30*24*60*60*1000" | bc`
RETENTION_MS[$TOPIC_AUDIT_TRAIL]=`echo "10*60*1000" | bc`
RETENTION_MS[$TOPIC_NOTIFICATIONS]=`echo "30*24*60*60*1000" | bc`
RETENTION_MS[$TOPIC_BACK_PATCH_DWH]=`echo "30*24*60*60*1000" | bc`
RETENTION_MS[$TOPIC_SYNC_PATIENTS_DWH]=`echo "30*24*60*60*1000" | bc`

declare -A SEGMENT_BYTES
SEGMENT_BYTES[$TOPIC_INTERACTION_ASYNC_ETL]=`echo "4*1024*1024" | bc`
SEGMENT_BYTES[$TOPIC_INTERACTION_CONTROLLER]=`echo "4*1024*1024" | bc`
SEGMENT_BYTES[$TOPIC_INTERACTION_EM]=`echo "4*1024*1024" | bc`
SEGMENT_BYTES[$TOPIC_INTERACTION_LINKER]=`echo "4*1024*1024" | bc`
SEGMENT_BYTES[$TOPIC_MU_LINKER]=`echo "4*1024*1024" | bc`
SEGMENT_BYTES[$TOPIC_AUDIT_TRAIL]=`echo "1*1024*1024" | bc`
SEGMENT_BYTES[$TOPIC_NOTIFICATIONS]=`echo "4*1024*1024" | bc`
SEGMENT_BYTES[$TOPIC_BACK_PATCH_DWH]=`echo "4*1024*1024" | bc`
SEGMENT_BYTES[$TOPIC_SYNC_PATIENTS_DWH]=`echo "4*1024*1024" | bc`
