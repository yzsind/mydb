#!/bin/sh
set -eu

default_config=/etc/mydb/mydb.cnf
has_explicit_config=false

for argument in "$@"; do
    case "$argument" in
        --config | --config=*)
            has_explicit_config=true
            break
            ;;
    esac
done

if [ "$has_explicit_config" = false ] && [ -f "$default_config" ]; then
    set -- --config "$default_config" "$@"
fi

exec /usr/local/bin/mydb "$@"
