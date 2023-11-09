#!/bin/bash

TOPIC_INTERACTION_ASYNC_ETL="JeMPI-async-etl"
TOPIC_INTERACTION_CONTROLLER="JeMPI-interaction-controller"
TOPIC_INTERACTION_EM="JeMPI-interaction-em"
TOPIC_INTERACTION_LINKER="JeMPI-interaction-linker"
TOPIC_MU_LINKER="JeMPI-mu-linker"
TOPIC_AUDIT_TRAIL="JeMPI-audit-trail"
TOPIC_NOTIFICATIONS="JeMPI-notifications"

declare -a TOPICS=(
  $TOPIC_INTERACTION_ASYNC_ETL
  $TOPIC_INTERACTION_CONTROLLER
  $TOPIC_INTERACTION_EM
  $TOPIC_INTERACTION_LINKER
  $TOPIC_MU_LINKER
  $TOPIC_AUDIT_TRAIL
  $TOPIC_NOTIFICATIONS
)

declare -A PARTITIONS
PARTITIONS[$TOPIC_INTERACTION_ASYNC_ETL]=1
PARTITIONS[$TOPIC_INTERACTION_CONTROLLER]=1
PARTITIONS[$TOPIC_INTERACTION_EM]=1
PARTITIONS[$TOPIC_INTERACTION_LINKER]=1
PARTITIONS[$TOPIC_MU_LINKER]=1
PARTITIONS[$TOPIC_AUDIT_TRAIL]=1
PARTITIONS[$TOPIC_NOTIFICATIONS]=1
  
declare -A REPLICATION
REPLICATION[$TOPIC_INTERACTION_ASYNC_ETL]=2
REPLICATION[$TOPIC_INTERACTION_CONTROLLER]=2
REPLICATION[$TOPIC_INTERACTION_EM]=2
REPLICATION[$TOPIC_INTERACTION_LINKER]=2
REPLICATION[$TOPIC_MU_LINKER]=2
REPLICATION[$TOPIC_AUDIT_TRAIL]=2
REPLICATION[$TOPIC_NOTIFICATIONS]=2

declare -A RETENTION_MS
RETENTION_MS[$TOPIC_INTERACTION_ASYNC_ETL]=`echo "1*24*60*60*1000" | bc`
RETENTION_MS[$TOPIC_INTERACTION_CONTROLLER]=`echo "1*24*60*60*1000" | bc`
RETENTION_MS[$TOPIC_INTERACTION_EM]=`echo "1*24*60*60*1000" | bc`
RETENTION_MS[$TOPIC_INTERACTION_LINKER]=`echo "1*24*60*60*1000" | bc`
RETENTION_MS[$TOPIC_MU_LINKER]=`echo "1*24*60*60*1000" | bc`
RETENTION_MS[$TOPIC_AUDIT_TRAIL]=`echo "10*60*1000" | bc`
RETENTION_MS[$TOPIC_NOTIFICATIONS]=`echo "1*24*60*60*1000" | bc`

declare -A SEGMENT_BYTES
SEGMENT_BYTES[$TOPIC_INTERACTION_ASYNC_ETL]=`echo "4*1024*1024" | bc`
SEGMENT_BYTES[$TOPIC_INTERACTION_CONTROLLER]=`echo "4*1024*1024" | bc`
SEGMENT_BYTES[$TOPIC_INTERACTION_EM]=`echo "4*1024*1024" | bc`
SEGMENT_BYTES[$TOPIC_INTERACTION_LINKER]=`echo "4*1024*1024" | bc`
SEGMENT_BYTES[$TOPIC_MU_LINKER]=`echo "4*1024*1024" | bc`
SEGMENT_BYTES[$TOPIC_AUDIT_TRAIL]=`echo "1*1024*1024" | bc`
SEGMENT_BYTES[$TOPIC_NOTIFICATIONS]=`echo "4*1024*1024" | bc`
