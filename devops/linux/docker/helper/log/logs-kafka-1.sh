#!/bin/bash

set -e
set -u

source ../../conf.env
docker service logs --follow --raw ${STACK_NAME}_kafka-01
echo
