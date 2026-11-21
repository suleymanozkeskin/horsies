#!/usr/bin/env bash
# Rebuilds core/webui-dist from webui/ (`bun run build`)
# and fails when the result differs from the committed build: a changed or
# deleted tracked file, or a file that git does not track (ignored files
# included).
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
dist_dir="core/webui-dist"

cd "$repo_root/webui"
bun run build

cd "$repo_root"
changed="$(git diff --name-status -- "$dist_dir")"
untracked="$(git ls-files --others -- "$dist_dir")"
if [[ -n "$changed" || -n "$untracked" ]]; then
  echo "$dist_dir is stale; run 'cd webui && bun run build' and commit the result" >&2
  git status --short --untracked-files=all --ignored -- "$dist_dir" >&2
  exit 1
fi
echo "$dist_dir matches the build of webui/"
