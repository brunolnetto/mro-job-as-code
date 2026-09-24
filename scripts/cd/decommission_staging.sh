#!/usr/bin/env bash
set -euo pipefail

# One-time helper for repositories that previously deployed STAGING.
# Run this while authenticated to the former STAGING Databricks workspace.
# It pauses any legacy STAGING simulator/analytics jobs so removed workflow
# routes cannot leave stale schedules consuming resources.

legacy_jobs=(
  "mro-source-simulator-staging-blue"
  "mro-analytics-pipeline-staging-blue"
  "mro-source-simulator-staging-green"
  "mro-analytics-pipeline-staging-green"
)

job_id_by_name(){
  local name="$1" payload
  payload="$(databricks jobs list --name "$name" -o json)"
  jq -r --arg name "$name" '
    (if type=="array" then . else (.jobs//[]) end)
    | map(select(.settings.name==$name))
    | if length==0 then "" elif length==1 then (.[0].job_id|tostring) else error("duplicate job names") end
  ' <<<"$payload"
}

pause_job(){
  local name="$1" id current payload
  id="$(job_id_by_name "$name")"

  if [[ -z "$id" ]]; then
    echo "Legacy STAGING job not found: $name"
    return 0
  fi

  current="$(databricks jobs get "$id" -o json)"
  payload="$(jq '
    {
      new_settings: {
        schedule: (.settings.schedule | .pause_status="PAUSED"),
        tags: ((.settings.tags // {}) + {
          deployment_active: "false",
          legacy_environment: "staging-decommissioned"
        })
      }
    }
  ' <<<"$current")"

  databricks jobs update "$id" --json "$payload" >/dev/null
  echo "Paused legacy STAGING job: $name (job_id=$id)"
}

databricks current-user me >/dev/null

for name in "${legacy_jobs[@]}"; do
  pause_job "$name"
done

echo "Legacy STAGING schedules are paused."
echo "Verify them in Databricks, then remove the old GitHub staging Environment if it still exists."
