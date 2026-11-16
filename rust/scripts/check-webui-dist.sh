#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"

cd "$repo_root/rust/webui"
bun run build

cd "$repo_root"
if ! git diff --quiet -- crates/horsies/webui-dist || \
  [[ -n "$(git ls-files --others --exclude-standard -- crates/horsies/webui-dist)" ]]; then
  echo "crates/horsies/webui-dist is stale; run 'cd rust/webui && bun run build' and commit the result" >&2
  git status --short --untracked-files=all -- crates/horsies/webui-dist >&2
  exit 1
fi
