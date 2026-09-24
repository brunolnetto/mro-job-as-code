#!/usr/bin/env bash
set -euo pipefail

analytics_job_name() {
  local env="$1" slot="$2"
  printf 'mro-analytics-pipeline-%s-%s' "$env" "$slot"
}

simulator_job_name() {
  local env="$1" slot="$2"
  printf 'mro-source-simulator-%s-%s' "$env" "$slot"
}

job_id_by_name() {
  local name="$1" payload
  payload="$(databricks jobs list --name "$name" -o json)"

  jq -r --arg name "$name" '
    (if type == "array" then . else (.jobs // []) end)
    | map(select(.settings.name == $name))
    | if length == 0 then ""
      elif length == 1 then (.[0].job_id | tostring)
      else error("multiple jobs with the same deployment name")
      end
  ' <<<"$payload"
}

job_json_by_name() {
  local name="$1" id
  id="$(job_id_by_name "$name")"
  if [[ -z "$id" ]]; then
    return 1
  fi
  databricks jobs get "$id" -o json
}

job_pause_status() {
  local name="$1" payload
  if ! payload="$(job_json_by_name "$name")"; then
    printf 'MISSING\n'
    return 0
  fi

  jq -r '
    if .settings.schedule == null
    then "NO_SCHEDULE"
    else (.settings.schedule.pause_status // "NO_SCHEDULE")
    end
  ' <<<"$payload"
}

job_active_marker() {
  local name="$1" payload
  if ! payload="$(job_json_by_name "$name")"; then
    printf 'MISSING\n'
    return 0
  fi

  jq -r '.settings.tags.deployment_active // "false"' <<<"$payload"
}

slot_schedule_status() {
  local env="$1" slot="$2"
  local analytics_status simulator_status
  analytics_status="$(job_pause_status "$(analytics_job_name "$env" "$slot")")"
  simulator_status="$(job_pause_status "$(simulator_job_name "$env" "$slot")")"

  if [[ "$analytics_status" == "MISSING" && "$simulator_status" == "MISSING" ]]; then
    printf 'MISSING\n'
    return 0
  fi

  if [[ "$analytics_status" == "MISSING" || "$simulator_status" == "MISSING" ]]; then
    printf 'INCONSISTENT\n'
    return 0
  fi

  if [[ "$analytics_status" != "$simulator_status" ]]; then
    printf 'INCONSISTENT\n'
    return 0
  fi

  if [[ "$analytics_status" != "PAUSED" && "$analytics_status" != "UNPAUSED" ]]; then
    printf 'INCONSISTENT\n'
    return 0
  fi

  printf '%s\n' "$analytics_status"
}

slot_marker_status() {
  local env="$1" slot="$2"
  local analytics_marker simulator_marker
  analytics_marker="$(job_active_marker "$(analytics_job_name "$env" "$slot")")"
  simulator_marker="$(job_active_marker "$(simulator_job_name "$env" "$slot")")"

  if [[ "$analytics_marker" == "MISSING" && "$simulator_marker" == "MISSING" ]]; then
    printf 'MISSING\n'
    return 0
  fi

  if [[ "$analytics_marker" == "MISSING" || "$simulator_marker" == "MISSING" ]]; then
    printf 'INCONSISTENT\n'
    return 0
  fi

  if [[ "$analytics_marker" != "$simulator_marker" ]]; then
    printf 'INCONSISTENT\n'
    return 0
  fi

  case "$analytics_marker" in
    true)  printf 'ACTIVE\n' ;;
    false) printf 'INACTIVE\n' ;;
    *)     printf 'INCONSISTENT\n' ;;
  esac
}

set_job_pause_status() {
  local name="$1" status="$2"
  local id job_json payload

  id="$(job_id_by_name "$name")"
  if [[ -z "$id" ]]; then
    echo "Job not found: $name" >&2
    return 1
  fi

  job_json="$(databricks jobs get "$id" -o json)"
  payload="$(
    jq --arg status "$status" '
      {new_settings: {schedule: (.settings.schedule | .pause_status = $status)}}
    ' <<<"$job_json"
  )"

  databricks jobs update "$id" --json "$payload" >/dev/null
}

set_job_active_marker() {
  local name="$1" active="$2"
  local id job_json payload

  id="$(job_id_by_name "$name")"
  if [[ -z "$id" ]]; then
    echo "Job not found: $name" >&2
    return 1
  fi

  job_json="$(databricks jobs get "$id" -o json)"
  payload="$(
    jq --arg active "$active" '
      {new_settings: {tags: ((.settings.tags // {}) + {deployment_active: $active})}}
    ' <<<"$job_json"
  )"

  databricks jobs update "$id" --json "$payload" >/dev/null
}

mark_slot() {
  local env="$1" slot="$2" active="$3"
  set_job_active_marker "$(simulator_job_name "$env" "$slot")" "$active"
  set_job_active_marker "$(analytics_job_name "$env" "$slot")" "$active"
}

active_run_count() {
  local name="$1" id payload
  id="$(job_id_by_name "$name")"
  if [[ -z "$id" ]]; then
    echo "Job not found: $name" >&2
    return 1
  fi

  payload="$(databricks jobs list-runs --job-id "$id" --active-only -o json)"
  jq -r 'if type == "array" then length else ((.runs // []) | length) end' <<<"$payload"
}

wait_job_idle() {
  local name="$1" attempt count

  for attempt in $(seq 1 80); do
    count="$(active_run_count "$name")"
    if [[ "$count" == "0" ]]; then
      return 0
    fi
    sleep 15
  done

  echo "Timed out waiting for job to become idle: $name" >&2
  return 1
}

pause_slot_if_present() {
  local env="$1" slot="$2" sim analytics
  sim="$(simulator_job_name "$env" "$slot")"
  analytics="$(analytics_job_name "$env" "$slot")"

  if [[ -n "$(job_id_by_name "$sim")" ]]; then
    set_job_pause_status "$sim" PAUSED
    wait_job_idle "$sim"
  fi

  if [[ -n "$(job_id_by_name "$analytics")" ]]; then
    set_job_pause_status "$analytics" PAUSED
    wait_job_idle "$analytics"
  fi
}

pause_slot() {
  local env="$1" slot="$2"
  set_job_pause_status "$(simulator_job_name "$env" "$slot")" PAUSED
  set_job_pause_status "$(analytics_job_name "$env" "$slot")" PAUSED
  wait_job_idle "$(simulator_job_name "$env" "$slot")"
  wait_job_idle "$(analytics_job_name "$env" "$slot")"
}

unpause_slot() {
  local env="$1" slot="$2"
  set_job_pause_status "$(simulator_job_name "$env" "$slot")" UNPAUSED
  set_job_pause_status "$(analytics_job_name "$env" "$slot")" UNPAUSED
}

assert_slot_status() {
  local env="$1" slot="$2" expected="$3" actual
  actual="$(slot_schedule_status "$env" "$slot")"
  if [[ "$actual" != "$expected" ]]; then
    echo "Expected $env/$slot schedules to be $expected, got $actual" >&2
    return 1
  fi
}

assert_slot_marker() {
  local env="$1" slot="$2" expected="$3" actual
  actual="$(slot_marker_status "$env" "$slot")"
  if [[ "$actual" != "$expected" ]]; then
    echo "Expected $env/$slot marker to be $expected, got $actual" >&2
    return 1
  fi
}

quiesce_dev() {
  pause_slot_if_present dev blue
  pause_slot_if_present dev green
}

discover_dev() {
  local blue green active candidate
  blue="$(slot_marker_status dev blue)"
  green="$(slot_marker_status dev green)"

  if [[ "$blue" == "INCONSISTENT" || "$green" == "INCONSISTENT" ]]; then
    echo "Blue/green active markers are inconsistent for dev." >&2
    echo "blue=$blue green=$green" >&2
    return 1
  fi

  if [[ "$blue" == "ACTIVE" && "$green" == "ACTIVE" ]]; then
    echo "Both DEV slots are marked active; refusing to continue." >&2
    return 1
  fi

  if [[ "$blue" == "ACTIVE" ]]; then
    active=blue
    candidate=green
  elif [[ "$green" == "ACTIVE" ]]; then
    active=green
    candidate=blue
  elif [[ "$blue" == "MISSING" && "$green" == "MISSING" ]]; then
    active=none
    candidate=blue
  elif [[ "$blue" == "INACTIVE" && "$green" == "MISSING" ]]; then
    active=none
    candidate=blue
  elif [[ "$green" == "INACTIVE" && "$blue" == "MISSING" ]]; then
    active=none
    candidate=green
  else
    # Both slots may exist but be inactive after an interrupted deployment.
    # DEV has no live traffic, so deterministically restart from blue.
    active=none
    candidate=blue
  fi

  printf 'ACTIVE_SLOT=%q\n' "$active"
  printf 'CANDIDATE_SLOT=%q\n' "$candidate"
}

discover_prod() {
  local blue green active candidate
  blue="$(slot_schedule_status prod blue)"
  green="$(slot_schedule_status prod green)"

  if [[ "$blue" == "INCONSISTENT" || "$green" == "INCONSISTENT" ]]; then
    echo "Blue/green schedule state is inconsistent for prod." >&2
    echo "blue=$blue green=$green" >&2
    return 1
  fi

  if [[ "$blue" == "UNPAUSED" && "$green" == "UNPAUSED" ]]; then
    echo "Both PROD slots are active; refusing to continue." >&2
    return 1
  fi

  if [[ "$blue" == "UNPAUSED" ]]; then
    active=blue
    candidate=green
  elif [[ "$green" == "UNPAUSED" ]]; then
    active=green
    candidate=blue
  elif [[ "$blue" == "MISSING" && "$green" == "MISSING" ]]; then
    active=none
    candidate=blue
  elif [[ "$blue" == "PAUSED" && "$green" == "MISSING" ]]; then
    active=none
    candidate=blue
  elif [[ "$green" == "PAUSED" && "$blue" == "MISSING" ]]; then
    active=none
    candidate=green
  else
    echo "Neither PROD slot is active and the state is ambiguous." >&2
    echo "blue=$blue green=$green" >&2
    return 1
  fi

  printf 'ACTIVE_SLOT=%q\n' "$active"
  printf 'CANDIDATE_SLOT=%q\n' "$candidate"
}

discover() {
  local env="$1"
  case "$env" in
    dev) discover_dev ;;
    prod) discover_prod ;;
    *) echo "Unknown environment: $env" >&2; return 2 ;;
  esac
}

promote_dev() {
  local active="$1" candidate="$2"

  # DEV is validation-only: all schedules remain paused and no stable source
  # is allowed to run. The active marker only identifies the latest validated slot.
  quiesce_dev
  assert_slot_status dev "$candidate" PAUSED

  if [[ "$active" != "none" ]]; then
    mark_slot dev "$active" false
  fi
  mark_slot dev "$candidate" true

  assert_slot_marker dev "$candidate" ACTIVE
  assert_slot_status dev "$candidate" PAUSED

  if [[ "$active" != "none" ]]; then
    assert_slot_marker dev "$active" INACTIVE
    assert_slot_status dev "$active" PAUSED
  fi

  echo "Promoted DEV validation marker from $active to $candidate; schedules remain PAUSED"
}

promote_prod() {
  local active="$1" candidate="$2"

  assert_slot_status prod "$candidate" PAUSED

  rollback_switch() {
    set +e
    pause_slot prod "$candidate" >/dev/null 2>&1 || true
    mark_slot prod "$candidate" false >/dev/null 2>&1 || true
    if [[ "$active" != "none" ]]; then
      unpause_slot prod "$active" >/dev/null 2>&1 || true
      mark_slot prod "$active" true >/dev/null 2>&1 || true
    fi
  }
  trap rollback_switch ERR

  if [[ "$active" != "none" ]]; then
    pause_slot prod "$active"
  fi

  unpause_slot prod "$candidate"
  mark_slot prod "$candidate" true

  if [[ "$active" != "none" ]]; then
    mark_slot prod "$active" false
  fi

  assert_slot_status prod "$candidate" UNPAUSED
  assert_slot_marker prod "$candidate" ACTIVE

  if [[ "$active" != "none" ]]; then
    assert_slot_status prod "$active" PAUSED
    assert_slot_marker prod "$active" INACTIVE
  fi

  trap - ERR
  echo "Promoted PROD from $active to $candidate"
}

promote() {
  local env="$1" active="$2" candidate="$3"
  case "$env" in
    dev) promote_dev "$active" "$candidate" ;;
    prod) promote_prod "$active" "$candidate" ;;
    *) echo "Unknown environment: $env" >&2; return 2 ;;
  esac
}

usage() {
  cat >&2 <<'EOF_USAGE'
Usage:
  blue_green.sh discover <dev|prod>
  blue_green.sh assert-status <dev|prod> <blue|green> <PAUSED|UNPAUSED>
  blue_green.sh quiesce-dev
  blue_green.sh promote <dev|prod> <blue|green|none> <blue|green>
EOF_USAGE
}

command="${1:-}"
case "$command" in
  discover)
    [[ $# -eq 2 ]] || { usage; exit 2; }
    discover "$2"
    ;;
  assert-status)
    [[ $# -eq 4 ]] || { usage; exit 2; }
    assert_slot_status "$2" "$3" "$4"
    ;;
  quiesce-dev)
    [[ $# -eq 1 ]] || { usage; exit 2; }
    quiesce_dev
    ;;
  promote)
    [[ $# -eq 4 ]] || { usage; exit 2; }
    promote "$2" "$3" "$4"
    ;;
  *)
    usage
    exit 2
    ;;
esac
