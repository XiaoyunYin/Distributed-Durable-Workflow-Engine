#!/usr/bin/env bash
set -euo pipefail
tool="$(basename "$0")"
if [[ -n "${DUR050_ARGV_LOG:-}" ]]; then
  python3 -c 'import json,os,sys; open(os.environ["DUR050_ARGV_LOG"],"a",encoding="utf-8").write(json.dumps([os.path.basename(sys.argv[1]),*sys.argv[2:]])+"\n")' "$0" "$@"
fi
if [[ "$tool" == docker && -n "${DUR050_DOCKER_LOG:-}" ]]; then
  python3 -c 'import json,os,sys; open(os.environ["DUR050_DOCKER_LOG"],"a",encoding="utf-8").write(json.dumps(["docker",*sys.argv[1:]])+"\n")' "$@"
fi
args="$*"
case "$tool" in
  grep) exec /usr/bin/grep "$@" ;;
  sha256sum) exec /usr/bin/sha256sum "$@" ;;
  bash) exec /bin/bash "$@" ;;
  psql)
    if [[ "$args" == *'SELECT json_build_object'* ]]; then echo '{"schema_version":18,"pg_stat_statements_preloaded":true,"pg_stat_statements_installed":true,"definition_ids":["dur050-fanout-8-v1@1","dur050-seq-8-v1@1"],"workflows":0,"attempts":0,"outbox":0,"inbox":0}'
    elif [[ "$args" == *'max(version)'* ]]; then echo 18
    elif [[ "$args" == *'current_setting'* ]]; then echo t
    elif [[ "$args" == *'schema_migrations'* ]]; then echo 18
    else echo '{"database_size_bytes":1,"key_tables":{},"consumer_offsets":[],"pg_stat_statements":{}}'
    fi ;;
  kafka-topics.sh) printf '%s\n' '__consumer_offsets' 'durable-agent.events.v1' 'durable-agent.tasks.v1' ;;
  kafka-consumer-groups.sh)
    group='runtime-workers-v1'
    while (($#)); do if [[ "$1" == --group ]]; then group="$2"; shift 2; else shift; fi; done
    printf '%s client host 1 1 consumer\n' "$group" ;;
  docker)
    if [[ "$args" == *'/dur050-fixture '* ]]; then
      python3 -c 'import json,os; print(json.dumps({"api_url":os.environ["DUR050_TEST_API_URL"],"namespace":os.environ["DUR050_TEST_NAMESPACE"],"run_id":"pilot-calibration","seed":50050,"families":[{"name":"seq-8","definition_id":"dur050-seq-8-v1","definition_version":1},{"name":"fanout-8","definition_id":"dur050-fanout-8-v1","definition_version":1}]}))'
    elif [[ "$args" == *'psql'* ]]; then
      for ((i=1;i<=$#;i++)); do
        if [[ "${!i}" == psql ]]; then shift $((i-1)); exec psql "$@"; fi
      done
      echo 'docker stub could not extract psql argv' >&2; exit 87
    elif [[ "$args" == *'pg_isready'* ]]; then echo 'accepting connections'
    elif [[ "$args" == *'kafka-topics.sh'* ]]; then kafka-topics.sh --list
    elif [[ "$args" == *'kafka-consumer-groups.sh'* ]]; then
      group='runtime-workers-v1'
      for ((i=1;i<=$#;i++)); do if [[ "${!i}" == --group ]]; then j=$((i+1)); group="${!j}"; fi; done
      kafka-consumer-groups.sh --group "$group"
    elif [[ "$args" == *'ps --services --filter status=running'* ]]; then :
    elif [[ "$args" == *'ps -aq kafka-init'* ]]; then echo init-container
    elif [[ "$args" == *'ps -q runtime worker'* ]]; then :
    elif [[ "$args" == *'ps -q runtime'* ]]; then echo runtime-container
    elif [[ "$args" == *'inspect'* && "$args" == *'State.Status'* ]]; then echo 'exited:0'
    elif [[ "$args" == *'inspect'* && "$args" == *'.Config.Env'* ]]; then
      env_file="${DUR050_REMOTE_REPO_ROOT:-}/deploy/aws/.env"
      if [[ -f "$env_file" ]]; then cat "$env_file"; else printf '%s\n' 'DUR050_ADMISSION_MAX_ACTIVE=1000' 'DUR050_ADMISSION_MAX_PENDING_OUTBOX=50000' 'DUR050_RECORD_TRANSACTION_TIMINGS=0' 'DUR049_RECORD_LEASE_ACQUISITIONS=0'; fi
    elif [[ "$args" == *'volume inspect'* ]]; then exit 1
    elif [[ "$args" == *'run --rm'* ]]; then
      mount=''
      previous=''
      for value in "$@"; do
        if [[ "$previous" == -v && "$value" == *:/backup ]]; then mount="${value%:/backup}"; fi
        previous="$value"
      done
      if [[ -n "$mount" ]]; then
        mkdir -p "$mount"
        if [[ "$args" == *'postgres-data.tar'* ]]; then cp "$DUR050_TEST_ARCHIVE_ROOT/postgres-data.tar" "$mount/postgres-data.tar"; else cp "$DUR050_TEST_ARCHIVE_ROOT/kafka-data.tar" "$mount/kafka-data.tar"; fi
      fi
    else :
    fi ;;
  cloud-init) printf 'status: done\nerrors: []\nrecoverable_errors: {}\n' ;;
  *) echo "unexpected stub invocation: $tool $args" >&2; exit 88 ;;
esac
