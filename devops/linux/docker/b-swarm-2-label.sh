#!/bin/bash

set -e
set -m

source ./0-conf.env

# docker node update --label-add name=$NODE3 $NODE3
docker node update --label-add name=$NODE2 $NODE2
docker node update --label-add name=$NODE1 $NODE1
