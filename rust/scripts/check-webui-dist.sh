#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"

cd "$repo_root/webui"
bun run build

cd "$repo_root"
if ! git diff --quiet -- horsies/webui-dist || \
  [[ -n "$(git ls-files --others --exclude-standard -- horsies/webui-dist)" ]]; then
  echo "horsies/webui-dist is stale; run 'cd webui && bun run build' and commit the result" >&2
  git status --short --untracked-files=all -- horsies/webui-dist >&2
  exit 1
fi
