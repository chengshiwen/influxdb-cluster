#!/bin/bash
set -e

export INFLUXDB_HOSTNAME=${INFLUXDB_HOSTNAME:-$HOSTNAME}
export INFLUXDB_META_CONFIG_PATH=${INFLUXDB_META_CONFIG_PATH:-/etc/influxdb/influxdb-meta.conf}

if [ "${1:0:1}" = '-' ]; then
    set -- influxd-meta "$@"
fi

if [ "$1" = 'influxd-meta' ]; then
    shift
    if [ $# -gt 0 ]; then
        case $1 in
          config)
            shift
            set -- influxd-meta config -config "${INFLUXDB_META_CONFIG_PATH}" "$@"
            ;;
          run)
            shift
            set -- influxd-meta run -config "${INFLUXDB_META_CONFIG_PATH}" "$@"
            ;;
          -*)
            set -- influxd-meta -config "${INFLUXDB_META_CONFIG_PATH}" "$@"
            ;;
          *)
            set -- influxd-meta "$@"
            ;;
        esac
    else
        set -- influxd-meta -config "${INFLUXDB_META_CONFIG_PATH}"
    fi
fi

exec "$@"
