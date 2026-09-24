#!/usr/bin/env bash
set -euo pipefail

analytics_job_name(){ printf 'mro-analytics-pipeline-%s-%s' "$1" "$2"; }
simulator_job_name(){ printf 'mro-source-simulator-%s-%s' "$1" "$2"; }

job_id_by_name(){
  local name="$1" payload
  payload="$(databricks jobs list --name "$name" -o json)"
  jq -r --arg name "$name" '(if type=="array" then . else (.jobs//[]) end)|map(select(.settings.name==$name))|if length==0 then "" elif length==1 then (.[0].job_id|tostring) else error("duplicate job names") end' <<<"$payload"
}

job_json_by_name(){
  local id
  id="$(job_id_by_name "$1")"
  [[ -n "$id" ]] || return 1
  databricks jobs get "$id" -o json
}

job_pause_status(){
  local p
  if ! p="$(job_json_by_name "$1")"; then
    echo MISSING
    return
  fi
  jq -r 'if .settings.schedule==null then "NO_SCHEDULE" else (.settings.schedule.pause_status//"NO_SCHEDULE") end' <<<"$p"
}

job_active_marker(){
  local p
  if ! p="$(job_json_by_name "$1")"; then
    echo MISSING
    return
  fi
  jq -r '.settings.tags.deployment_active//"false"' <<<"$p"
}

slot_schedule_status(){
  local env="$1" slot="$2" a s
  a="$(job_pause_status "$(analytics_job_name "$env" "$slot")")"
  s="$(job_pause_status "$(simulator_job_name "$env" "$slot")")"

  [[ "$a" == "$s" ]] || {
    echo INCONSISTENT
    return
  }

  case "$a" in
    PAUSED|UNPAUSED|MISSING) echo "$a" ;;
    *) echo INCONSISTENT ;;
  esac
}

slot_marker_status(){
  local env="$1" slot="$2" a s
  a="$(job_active_marker "$(analytics_job_name "$env" "$slot")")"
  s="$(job_active_marker "$(simulator_job_name "$env" "$slot")")"

  [[ "$a" == "$s" ]] || {
    echo INCONSISTENT
    return
  }

  case "$a" in
    true) echo ACTIVE ;;
    false) echo INACTIVE ;;
    MISSING) echo MISSING ;;
    *) echo INCONSISTENT ;;
  esac
}

set_job_pause_status(){
  local name="$1" status="$2" id j payload
  id="$(job_id_by_name "$name")"
  [[ -n "$id" ]] || {
    echo "Job not found: $name" >&2
    return 1
  }

  j="$(databricks jobs get "$id" -o json)"
  payload="$(jq --arg status "$status" '{new_settings:{schedule:(.settings.schedule|.pause_status=$status)}}' <<<"$j")"
  databricks jobs update "$id" --json "$payload" >/dev/null
}

set_job_active_marker(){
  local name="$1" active="$2" id j payload
  id="$(job_id_by_name "$name")"
  [[ -n "$id" ]] || return 1

  j="$(databricks jobs get "$id" -o json)"
  payload="$(jq --arg active "$active" '{new_settings:{tags:((.settings.tags//{})+{deployment_active:$active})}}' <<<"$j")"
  databricks jobs update "$id" --json "$payload" >/dev/null
}

mark_slot(){
  set_job_active_marker "$(simulator_job_name "$1" "$2")" "$3"
  set_job_active_marker "$(analytics_job_name "$1" "$2")" "$3"
}

active_run_count(){
  local id p
  id="$(job_id_by_name "$1")"
  [[ -n "$id" ]] || {
    echo 0
    return
  }

  p="$(databricks jobs list-runs --job-id "$id" --active-only -o json)"
  jq -r 'if type=="array" then length else ((.runs//[])|length) end' <<<"$p"
}

wait_job_idle(){
  for _ in $(seq 1 80); do
    [[ "$(active_run_count "$1")" == 0 ]] && return 0
    sleep 15
  done

  echo "Timed out waiting for idle job: $1" >&2
  return 1
}

pause_slot_if_present(){
  local env="$1" slot="$2" name
  for name in "$(simulator_job_name "$env" "$slot")" "$(analytics_job_name "$env" "$slot")"; do
    if [[ -n "$(job_id_by_name "$name")" ]]; then
      set_job_pause_status "$name" PAUSED
      wait_job_idle "$name"
    fi
  done
}

pause_slot(){ pause_slot_if_present "$1" "$2"; }

unpause_slot(){
  set_job_pause_status "$(simulator_job_name "$1" "$2")" UNPAUSED
  set_job_pause_status "$(analytics_job_name "$1" "$2")" UNPAUSED
}

assert_slot_status(){
  local actual
  actual="$(slot_schedule_status "$1" "$2")"
  [[ "$actual" == "$3" ]] || {
    echo "Expected $1/$2=$3, got $actual" >&2
    return 1
  }
}

discover_prod(){
  local b g bm gm
  b="$(slot_schedule_status prod blue)"
  g="$(slot_schedule_status prod green)"

  if [[ "$b" == INCONSISTENT || "$g" == INCONSISTENT ]]; then
    echo "Inconsistent PROD schedule state: blue=$b green=$g" >&2
    return 1
  fi

  case "$b/$g" in
    UNPAUSED/PAUSED|UNPAUSED/MISSING)
      echo 'ACTIVE_SLOT=blue'
      echo 'CANDIDATE_SLOT=green'
      ;;
    PAUSED/UNPAUSED|MISSING/UNPAUSED)
      echo 'ACTIVE_SLOT=green'
      echo 'CANDIDATE_SLOT=blue'
      ;;
    MISSING/MISSING)
      echo 'ACTIVE_SLOT=none'
      echo 'CANDIDATE_SLOT=blue'
      ;;
    PAUSED/MISSING)
      echo 'ACTIVE_SLOT=none'
      echo 'CANDIDATE_SLOT=blue'
      ;;
    MISSING/PAUSED)
      echo 'ACTIVE_SLOT=none'
      echo 'CANDIDATE_SLOT=green'
      ;;
    PAUSED/PAUSED)
      bm="$(slot_marker_status prod blue)"
      gm="$(slot_marker_status prod green)"

      if [[ "$bm" == MISSING || "$gm" == MISSING ||
            "$bm" == INCONSISTENT || "$gm" == INCONSISTENT ]]; then
        echo "Uncertain PROD deployment markers while both schedules are paused: blue=$bm green=$gm" >&2
        return 1
      fi

      case "$bm/$gm" in
        ACTIVE/INACTIVE)
          echo 'ACTIVE_SLOT=blue'
          echo 'CANDIDATE_SLOT=green'
          ;;
        INACTIVE/ACTIVE)
          echo 'ACTIVE_SLOT=green'
          echo 'CANDIDATE_SLOT=blue'
          ;;
        INACTIVE/INACTIVE)
          echo 'ACTIVE_SLOT=none'
          echo 'CANDIDATE_SLOT=blue'
          ;;
        ACTIVE/ACTIVE)
          echo "Both PROD slots carry deployment_active=true while both schedules are paused" >&2
          return 1
          ;;
        *)
          echo "Ambiguous PROD deployment markers: blue=$bm green=$gm" >&2
          return 1
          ;;
      esac
      ;;
    UNPAUSED/UNPAUSED)
      echo "Both PROD slots are active" >&2
      return 1
      ;;
    *)
      echo "Ambiguous PROD schedule state: blue=$b green=$g" >&2
      return 1
      ;;
  esac
}

discover(){
  [[ "$1" == prod ]] || {
    echo "Blue-green discovery is only supported for PROD" >&2
    return 2
  }
  discover_prod
}

promote_prod(){
  local active="$1" candidate="$2"

  assert_slot_status prod "$candidate" PAUSED

  if [[ "$active" != none ]]; then
    pause_slot prod "$active"
  fi

  unpause_slot prod "$candidate"
  mark_slot prod "$candidate" true

  if [[ "$active" != none ]]; then
    mark_slot prod "$active" false
  fi

  assert_slot_status prod "$candidate" UNPAUSED
  echo "Promoted PROD from $active to $candidate"
}

promote(){
  [[ "$1" == prod ]] || {
    echo "Blue-green promotion is only supported for PROD" >&2
    return 2
  }
  promote_prod "$2" "$3"
}

case "${1:-}" in
  discover) discover "${2:-}" ;;
  assert-status) assert_slot_status "${2:-}" "${3:-}" "${4:-}" ;;
  promote) promote "${2:-}" "${3:-}" "${4:-}" ;;
  *)
    echo 'Usage: blue_green.sh {discover prod|assert-status ENV SLOT STATUS|promote prod ACTIVE CANDIDATE}' >&2
    exit 2
    ;;
esac
