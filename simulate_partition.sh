#!/usr/bin/env bash
set -euo pipefail

US_BASE_URL="${REGION_US_BASE_URL:-http://localhost:3001}"
EU_BASE_URL="${REGION_EU_BASE_URL:-http://localhost:3002}"
APAC_BASE_URL="${REGION_APAC_BASE_URL:-http://localhost:3003}"
WAIT_SECONDS="${WAIT_SECONDS:-30}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

need curl
need jq

call() {
  local method="$1"
  local url="$2"
  local body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -sS -X "$method" "$url" -H "Content-Type: application/json" -d "$body"
  else
    curl -sS -X "$method" "$url"
  fi
}

wait_for_json() {
  local url="$1"
  local jq_filter="$2"
  local expected="$3"
  local waited=0
  while (( waited < WAIT_SECONDS )); do
    local response
    response="$(curl -sS "$url" || true)"
    if [[ -n "$response" ]]; then
      local current
      current="$(printf '%s' "$response" | jq -r "$jq_filter" 2>/dev/null || true)"
      if [[ "$current" == "$expected" ]]; then
        return 0
      fi
    fi
    sleep 1
    waited=$((waited + 1))
  done
  echo "timed out waiting for $url to satisfy $jq_filter == $expected" >&2
  exit 1
}

echo "creating incident in region-us"
incident="$(call POST "$US_BASE_URL/incidents" '{"title":"Primary database outage","description":"Write latency is above SLA","severity":"HIGH"}')"
incident_id="$(printf '%s' "$incident" | jq -r '.id')"
echo "incident id: $incident_id"

echo "waiting for replication to region-eu and region-apac"
wait_for_json "$EU_BASE_URL/incidents/$incident_id" '.id' "$incident_id"
wait_for_json "$APAC_BASE_URL/incidents/$incident_id" '.id' "$incident_id"

echo "blocking replication between region-us and region-eu"
call POST "$US_BASE_URL/internal/peers/block" '{"peer_region":"eu"}' >/dev/null
call POST "$EU_BASE_URL/internal/peers/block" '{"peer_region":"us"}' >/dev/null

us_current="$(curl -sS "$US_BASE_URL/incidents/$incident_id")"
eu_current="$(curl -sS "$EU_BASE_URL/incidents/$incident_id")"
us_vc="$(printf '%s' "$us_current" | jq '.vector_clock')"
eu_vc="$(printf '%s' "$eu_current" | jq '.vector_clock')"

echo "updating region-us during partition"
us_update_payload="$(jq -n --argjson vc "$us_vc" '{status:"ACKNOWLEDGED",assigned_team:"SRE-Team-A",vector_clock:$vc}')"
call PUT "$US_BASE_URL/incidents/$incident_id" "$us_update_payload" >/dev/null

echo "updating region-eu during partition"
eu_update_payload="$(jq -n --argjson vc "$eu_vc" '{status:"CRITICAL",assigned_team:"SRE-Team-EU",vector_clock:$vc}')"
call PUT "$EU_BASE_URL/incidents/$incident_id" "$eu_update_payload" >/dev/null

echo "restoring replication between region-us and region-eu"
call POST "$US_BASE_URL/internal/peers/unblock" '{"peer_region":"eu"}' >/dev/null
call POST "$EU_BASE_URL/internal/peers/unblock" '{"peer_region":"us"}' >/dev/null

echo "waiting for version_conflict to be detected in region-eu"
wait_for_json "$EU_BASE_URL/incidents/$incident_id" '.version_conflict' "true"

echo "final conflicted incident from region-eu"
curl -sS "$EU_BASE_URL/incidents/$incident_id" | jq '.'
