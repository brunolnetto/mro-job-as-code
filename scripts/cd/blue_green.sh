#!/usr/bin/env bash
set -euo pipefail

analytics_job_name(){ printf 'mro-analytics-pipeline-%s-%s' "$1" "$2"; }
simulator_job_name(){ printf 'mro-source-simulator-%s-%s' "$1" "$2"; }

job_id_by_name(){
  local name="$1" payload
  payload="$(databricks jobs list --name "$name" -o json)"
  jq -r --arg name "$name" '(if type=="array" then . else (.jobs//[]) end)|map(select(.settings.name==$name))|if length==0 then "" elif length==1 then (.[0].job_id|tostring) else error("duplicate job names") end' <<<"$payload"
}
job_json_by_name(){ local id; id="$(job_id_by_name "$1")"; [[ -n "$id" ]] || return 1; databricks jobs get "$id" -o json; }
job_pause_status(){ local p; if ! p="$(job_json_by_name "$1")"; then echo MISSING; return; fi; jq -r 'if .settings.schedule==null then "NO_SCHEDULE" else (.settings.schedule.pause_status//"NO_SCHEDULE") end' <<<"$p"; }
job_active_marker(){ local p; if ! p="$(job_json_by_name "$1")"; then echo MISSING; return; fi; jq -r '.settings.tags.deployment_active//"false"' <<<"$p"; }

slot_schedule_status(){
  local env="$1" slot="$2" a s
  a="$(job_pause_status "$(analytics_job_name "$env" "$slot")")"; s="$(job_pause_status "$(simulator_job_name "$env" "$slot")")"
  [[ "$a" == "$s" ]] || { echo INCONSISTENT; return; }
  case "$a" in PAUSED|UNPAUSED|MISSING) echo "$a";; *) echo INCONSISTENT;; esac
}
slot_marker_status(){
  local env="$1" slot="$2" a s
  a="$(job_active_marker "$(analytics_job_name "$env" "$slot")")"; s="$(job_active_marker "$(simulator_job_name "$env" "$slot")")"
  [[ "$a" == "$s" ]] || { echo INCONSISTENT; return; }
  case "$a" in true) echo ACTIVE;; false) echo INACTIVE;; MISSING) echo MISSING;; *) echo INCONSISTENT;; esac
}
set_job_pause_status(){
  local name="$1" status="$2" id j payload
  id="$(job_id_by_name "$name")"; [[ -n "$id" ]] || { echo "Job not found: $name" >&2; return 1; }
  j="$(databricks jobs get "$id" -o json)"
  payload="$(jq --arg status "$status" '{new_settings:{schedule:(.settings.schedule|.pause_status=$status)}}' <<<"$j")"
  databricks jobs update "$id" --json "$payload" >/dev/null
}
set_job_active_marker(){
  local name="$1" active="$2" id j payload
  id="$(job_id_by_name "$name")"; [[ -n "$id" ]] || return 1
  j="$(databricks jobs get "$id" -o json)"
  payload="$(jq --arg active "$active" '{new_settings:{tags:((.settings.tags//{})+{deployment_active:$active})}}' <<<"$j")"
  databricks jobs update "$id" --json "$payload" >/dev/null
}
mark_slot(){ set_job_active_marker "$(simulator_job_name "$1" "$2")" "$3"; set_job_active_marker "$(analytics_job_name "$1" "$2")" "$3"; }
active_run_count(){ local id p; id="$(job_id_by_name "$1")"; [[ -n "$id" ]] || { echo 0; return; }; p="$(databricks jobs list-runs --job-id "$id" --active-only -o json)"; jq -r 'if type=="array" then length else ((.runs//[])|length) end' <<<"$p"; }
wait_job_idle(){ for _ in $(seq 1 80); do [[ "$(active_run_count "$1")" == 0 ]] && return 0; sleep 15; done; echo "Timed out: $1" >&2; return 1; }
pause_slot_if_present(){
  local env="$1" slot="$2" name
  for name in "$(simulator_job_name "$env" "$slot")" "$(analytics_job_name "$env" "$slot")"; do
    if [[ -n "$(job_id_by_name "$name")" ]]; then set_job_pause_status "$name" PAUSED; wait_job_idle "$name"; fi
  done
}
pause_slot(){ pause_slot_if_present "$1" "$2"; }
unpause_slot(){ set_job_pause_status "$(simulator_job_name "$1" "$2")" UNPAUSED; set_job_pause_status "$(analytics_job_name "$1" "$2")" UNPAUSED; }
assert_slot_status(){ local a; a="$(slot_schedule_status "$1" "$2")"; [[ "$a" == "$3" ]] || { echo "Expected $1/$2=$3, got $a" >&2; return 1; }; }

# DEV has no persistent runtime. The active marker means "last validated".
discover_dev(){
  local b g
  b="$(slot_marker_status dev blue)"
  g="$(slot_marker_status dev green)"

  case "$b/$g" in
    ACTIVE/INACTIVE|ACTIVE/MISSING)
      echo 'ACTIVE_SLOT=blue'
      echo 'CANDIDATE_SLOT=green'
      ;;
    INACTIVE/ACTIVE|MISSING/ACTIVE)
      echo 'ACTIVE_SLOT=green'
      echo 'CANDIDATE_SLOT=blue'
      ;;
    INACTIVE/INACTIVE|MISSING/MISSING)
      echo 'ACTIVE_SLOT=none'
      echo 'CANDIDATE_SLOT=blue'
      ;;
    ACTIVE/ACTIVE)
      echo 'Both DEV slots active' >&2
      return 1
      ;;
    *)
      echo "Ambiguous DEV blue/green marker state: blue=$b green=$g" >&2
      return 1
      ;;
  esac
}
# STAGING and PROD are persistent runtimes. Active means UNPAUSED schedules.
discover_runtime(){
  local env="$1" b g bm gm
  b="$(slot_schedule_status "$env" blue)"
  g="$(slot_schedule_status "$env" green)"

  # Any inconsistent schedule observation is fatal. Never select a candidate
  # from a partially known runtime state.
  if [[ "$b" == INCONSISTENT || "$g" == INCONSISTENT ]]; then
    echo "Inconsistent $env blue/green schedule state: blue=$b green=$g" >&2
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
      # Both schedule reads proved that both slots exist. Marker lookups must
      # therefore return ACTIVE or INACTIVE. MISSING now means lookup failure
      # or state loss, not "slot absent", and must fail closed.
      bm="$(slot_marker_status "$env" blue)"
      gm="$(slot_marker_status "$env" green)"

      if [[ "$bm" == MISSING || "$gm" == MISSING ||
            "$bm" == INCONSISTENT || "$gm" == INCONSISTENT ]]; then
        echo "Uncertain $env deployment markers while both schedules are paused: blue=$bm green=$gm" >&2
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
          echo "Both $env slots carry deployment_active=true while both schedules are paused" >&2
          return 1
          ;;
        *)
          echo "Ambiguous $env deployment markers while both schedules are paused: blue=$bm green=$gm" >&2
          return 1
          ;;
      esac
      ;;
    UNPAUSED/UNPAUSED)
      echo "Both $env slots active" >&2
      return 1
      ;;
    *)
      echo "Ambiguous $env blue/green schedule state: blue=$b green=$g" >&2
      return 1
      ;;
  esac
}

discover(){ case "$1" in dev) discover_dev;; staging|prod) discover_runtime "$1";; *) echo "Unknown environment: $1" >&2; return 2;; esac; }

promote_dev(){
  local active="$1" candidate="$2"
  pause_slot_if_present dev blue; pause_slot_if_present dev green
  [[ "$active" == none ]] || mark_slot dev "$active" false
  mark_slot dev "$candidate" true
  assert_slot_status dev "$candidate" PAUSED
  echo "DEV validated slot=$candidate; schedules remain PAUSED"
}
promote_runtime(){
  local env="$1" active="$2" candidate="$3"
  assert_slot_status "$env" "$candidate" PAUSED
  if [[ "$active" != none ]]; then pause_slot "$env" "$active"; fi
  unpause_slot "$env" "$candidate"; mark_slot "$env" "$candidate" true
  if [[ "$active" != none ]]; then mark_slot "$env" "$active" false; fi
  assert_slot_status "$env" "$candidate" UNPAUSED
  echo "Promoted $env from $active to $candidate"
}
promote(){ case "$1" in dev) promote_dev "$2" "$3";; staging|prod) promote_runtime "$1" "$2" "$3";; *) return 2;; esac; }

case "${1:-}" in
  discover) discover "$2";;
  assert-status) assert_slot_status "$2" "$3" "$4";;
  promote) promote "$2" "$3" "$4";;
  *) echo 'Usage: blue_green.sh {discover ENV|assert-status ENV SLOT STATUS|promote ENV ACTIVE CANDIDATE}' >&2; exit 2;;
esac
