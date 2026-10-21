#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
profile_name="vn30f1m"
profile_root="${CODEX_HOME:-$HOME/.codex}"
profile_source="$project_root/orderflow-demo/$profile_name.config.toml"
profile_target="$profile_root/$profile_name.config.toml"

if [[ "$project_root" != "/mnt/c/shanx2026/vn30f1m" ]]; then
  printf 'Profile is scoped to /mnt/c/shanx2026/vn30f1m; found %s\n' "$project_root" >&2
  exit 1
fi

mkdir -p -- "$profile_root"
if [[ -e "$profile_target" ]] && ! cmp -s -- "$profile_source" "$profile_target"; then
  printf 'Existing profile differs: %s\n' "$profile_target" >&2
  exit 1
fi
if [[ ! -e "$profile_target" ]]; then
  install -m 600 -- "$profile_source" "$profile_target"
fi

exec codex --no-daemon --profile "$profile_name" -C "$project_root" "$@"
