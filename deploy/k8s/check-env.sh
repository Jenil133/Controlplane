#!/usr/bin/env bash
# Checks the environment every overlay gives the server against the
# server's flags. The server reads CONTROLPLANE_<FLAG> for each flag (upper
# case, "-" as "_") and ignores every other name, so a misspelt name would
# quietly leave the server on a default. This fails when an overlay
#
#   - sets a CONTROLPLANE_* variable that matches no flag,
#   - leaves out one listed in "required" below,
#   - sets one to an empty string, or
#   - gives a bool or duration flag a value that does not parse.
#
# Needs go, kubectl and jq:
#
#   bash deploy/k8s/check-env.sh
set -euo pipefail

# Every overlay must set these. Without one the server falls back to its
# default, and several of those fail silently: no auth without
# AUTH_TOKENS_FILE, no cross-replica change events without REDIS_URL, a
# shutdown that outlasts the grace period without SHUTDOWN_TIMEOUT.
required=(
  CONTROLPLANE_GRPC_ADDR
  CONTROLPLANE_HTTP_ADDR
  CONTROLPLANE_STORE
  CONTROLPLANE_DATABASE_URL
  CONTROLPLANE_MIGRATE
  CONTROLPLANE_REDIS_URL
  CONTROLPLANE_RECONCILE_INTERVAL
  CONTROLPLANE_ROLLOUT_INTERVAL
  CONTROLPLANE_AUTH_TOKENS_FILE
  CONTROLPLANE_SHUTDOWN_TIMEOUT
  CONTROLPLANE_LOG_FORMAT
  CONTROLPLANE_LOG_LEVEL
)

# What strconv.ParseBool and time.ParseDuration accept, bar int64 overflow.
bool_re='^(1|t|T|TRUE|true|True|0|f|F|FALSE|false|False)$'
duration_re='^[-+]?(0|(([0-9]+(\.[0-9]*)?|\.[0-9]+)(ns|us|µs|μs|ms|s|m|h))+)$'

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
tmp="$(mktemp -d "${TMPDIR:-/tmp}/check-env.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT

go build -o "$tmp/controlplane" ./cmd/controlplane
# Only the usage text matters here, not the exit status. It shows each flag
# as "  -name type", or as "  -name" with no type for a bool. Lines out:
# CONTROLPLANE_NAME, a tab, the type.
"$tmp/controlplane" -h >"$tmp/usage" 2>&1 || true
awk '/^  -[a-z0-9-]+$/ || /^  -[a-z0-9-]+[ \t]/ {
  type = "bool"
  if (match($0, /^  -[a-z0-9-]+ /)) type = substr($0, RLENGTH + 1)
  name = toupper(substr($1, 2))
  gsub(/-/, "_", name)
  print "CONTROLPLANE_" name "\t" type
}' "$tmp/usage" >"$tmp/flags"
if [[ ! -s "$tmp/flags" ]]; then
  echo "no flags found in the output of controlplane -h:" >&2
  cat "$tmp/usage" >&2
  exit 1
fi

failed=0
for dir in deploy/k8s/overlays/*/; do
  overlay="$(basename "$dir")"
  # Lines out: NAME=VALUE for a literal value, NAME alone for a valueFrom
  # reference such as a Secret key. A no-op local patch is kubectl's offline
  # YAML-to-JSON conversion.
  kubectl kustomize "$dir" |
    kubectl patch --local -f - --type merge -p '{}' -o json |
    jq -r '
      if .kind == "ConfigMap" then
        .data // {} | to_entries[] | "\(.key)=\(.value)"
      else
        .spec.template.spec.containers[]?.env[]? |
          if has("value") then "\(.name)=\(.value)" else .name end
      end | select(startswith("CONTROLPLANE_"))' >"$tmp/env"

  while IFS= read -r line; do
    name="${line%%=*}"
    type="$(awk -F '\t' -v name="$name" '$1 == name { print $2 }' "$tmp/flags")"
    if [[ -z "$type" ]]; then
      echo "$overlay: $name matches no flag of the server" >&2
      failed=1
      continue
    fi
    [[ "$line" != "$name" ]] || continue
    value="${line#*=}"
    if [[ -z "$value" ]]; then
      echo "$overlay: $name is empty" >&2
      failed=1
      continue
    fi
    case "$type" in
      bool) re="$bool_re" ;;
      duration) re="$duration_re" ;;
      *) continue ;;
    esac
    if ! [[ $value =~ $re ]]; then
      echo "$overlay: $name=$value is not a valid $type" >&2
      failed=1
    fi
  done <"$tmp/env"

  for name in "${required[@]}"; do
    if ! grep -q -x -e "$name" -e "$name=.*" "$tmp/env"; then
      echo "$overlay: $name is not set" >&2
      failed=1
    fi
  done
  echo "$overlay: checked $(wc -l <"$tmp/env" | tr -d ' ') CONTROLPLANE_* variables"
done
exit "$failed"
