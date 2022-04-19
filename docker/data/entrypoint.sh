#!/bin/bash
set -e

export INFLUXDB_HOSTNAME=${INFLUXDB_HOSTNAME:-$HOSTNAME}

if [ "${1:0:1}" = '-' ]; then
    set -- influxd "$@"
fi

if [ "$1" = 'influxd' ]; then
	/init-influxdb.sh "${@:2}"
fi

exec "$@"
