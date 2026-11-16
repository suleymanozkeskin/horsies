#!/usr/bin/env bash
# Move the Python package to python/horsies/ and the baseline SQL files into
# the core crate, then check the result.
#
# Usage: scripts/package-move.sh [--commit] <checkout>
#
# <checkout> is a clean checkout of program/0.8.0. The script changes files
# only in <checkout> and stages every change there. With --commit, after every
# check passes, it writes two commits on the current branch: the renames with
# unchanged bytes, then the edits, so that git follows every moved file. It
# never pushes and never builds. It needs git and uv; uv supplies Python 3.13
# and PyYAML. Steps, checks and follow-up work: scripts/package-move/README.md.
set -euo pipefail

tool_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/package-move"
readonly tool_dir
readonly rules="$tool_dir/move_rules.py"
readonly sql_patch="$tool_dir/sql-through-core.patch"
readonly python_version='3.13'
readonly pyyaml_version='6.0.2'

fail() {
    echo "package-move: $*" >&2
    exit 1
}

step() {
    printf '\n== %s\n' "$*"
}

run_python() {
    uv run --quiet --no-project --no-config --python "$python_version" "$@"
}

commit='no'
if [ "$#" -eq 2 ] && [ "$1" = '--commit' ]; then
    commit='yes'
    shift
fi
if [ "$#" -ne 1 ]; then
    echo "usage: $0 [--commit] <checkout>" >&2
    exit 2
fi
readonly commit
[ -d "$1" ] || fail "$1 is not a folder"
repo="$(cd "$1" && pwd)"
readonly repo
export GIT_LITERAL_PATHSPECS=1

git_repo() {
    git -C "$repo" "$@"
}

step 'Preflight'
for tool in git uv; do
    command -v "$tool" >/dev/null || fail "$tool is not on PATH"
done
[ -f "$rules" ] || fail "missing $rules"
[ -f "$sql_patch" ] || fail "missing $sql_patch"
[ "$(git_repo rev-parse --show-toplevel)" = "$repo" ] || fail "$repo is not the top of a git checkout"
[ -z "$(git_repo status --porcelain)" ] || fail "$repo has changes or untracked files; start from a clean checkout"
[ -f "$repo/horsies/__init__.py" ] || fail 'no horsies/__init__.py: the package is not at the old path'
[ -d "$repo/horsies/core/baseline/sql" ] || fail 'no horsies/core/baseline/sql/'
[ ! -e "$repo/python" ] || fail 'python/ exists already'
[ ! -e "$repo/crates/horsies-core/baseline" ] || fail 'crates/horsies-core/baseline/ exists already'
[ -f "$repo/crates/horsies-python/Cargo.toml" ] || fail 'no crates/horsies-python/Cargo.toml'
grep -q '^build-backend = "setuptools.build_meta"$' "$repo/pyproject.toml" \
    || fail 'pyproject.toml does not build with setuptools; is the move done already?'
old_commit="$(git_repo rev-parse HEAD)"
readonly old_commit
echo "checkout: $repo"
echo "branch: $(git_repo branch --show-current)"
echo "commit before the move: $old_commit"

step 'Move the SQL files into the core crate'
git_repo mv horsies/core/baseline/sql crates/horsies-core/baseline
git_repo ls-files -- crates/horsies-core/baseline

step 'Move the package to python/horsies/'
mkdir "$repo/python"
git_repo mv horsies python/horsies
echo "tracked files under python/horsies/: $(git_repo ls-files -- python/horsies | wc -l | tr -d ' ')"

step 'Rewrite path references'
changed_list="$(mktemp)"
trap 'rm -f "$changed_list"' EXIT
run_python python -I "$rules" references "$repo" "$changed_list"
if [ -s "$changed_list" ]; then
    git_repo add --pathspec-from-file="$changed_list"
fi

step 'Switch pyproject.toml to maturin'
run_python python -I "$rules" pyproject "$repo"
git_repo add -- pyproject.toml

step 'Read the baseline SQL through horsies._core'
git_repo apply --index --check "$sql_patch" \
    || fail "$sql_patch does not apply; the branch moved on since the patch was made"
git_repo apply --stat "$sql_patch" | tail -n 1
git_repo apply --index "$sql_patch"

step 'Check'
git_repo diff --quiet || fail 'a change is not staged'
[ -z "$(git_repo ls-files --others --exclude-standard)" ] || fail 'an untracked file is left'
run_python python -I "$rules" check "$repo" "$old_commit" || fail 'a check failed (listed above)'
yaml_files=()
while IFS= read -r -d '' path; do
    case "$path" in
        *.yml | *.yaml) yaml_files+=("$repo/$path") ;;
    esac
done < <(git_repo ls-files -z)
[ "${#yaml_files[@]}" -gt 0 ] || fail 'no tracked YAML file found'
run_python --with "pyyaml==$pyyaml_version" python -I -c '
import sys
import yaml
for path in sys.argv[1:]:
    with open(path, encoding="utf-8") as handle:
        list(yaml.safe_load_all(handle))
print(f"YAML files parsed: {len(sys.argv) - 1}")
' "${yaml_files[@]}" || fail 'a YAML file does not parse'

step 'Summary'
renames="$(git_repo diff --cached -M --name-status | grep -c '^R' || true)"
exact="$(git_repo diff --cached -M --name-status | grep -c '^R100' || true)"
echo "renamed files in one diff: $renames ($exact unchanged, $((renames - exact)) with edits)"
git_repo diff --cached -M --shortstat

if [ "$commit" = 'no' ]; then
    echo
    echo 'The move is staged and checked. Review it with:'
    echo "    git -C '$repo' diff --cached -M --stat"
    echo 'Nothing is committed or pushed.'
    exit 0
fi

step 'Commit: renames, then edits'
readonly zero_blob='0000000000000000000000000000000000000000'
index_dir="$(mktemp -d)"
trap 'rm -f "$changed_list"; rm -rf "$index_dir"' EXIT
rename_index="$index_dir/index"
GIT_INDEX_FILE="$rename_index" git_repo read-tree "$old_commit"
git_repo ls-tree -r -z "$old_commit" -- horsies/ \
    | while IFS= read -r -d '' entry; do
        meta="${entry%%$'\t'*}"
        path="${entry#*$'\t'}"
        mode="${meta%% *}"
        blob="${meta##* }"
        case "$path" in
            horsies/core/baseline/sql/*) new_path="crates/horsies-core/baseline/${path#horsies/core/baseline/sql/}" ;;
            *) new_path="python/$path" ;;
        esac
        printf '0 %s\t%s\0' "$zero_blob" "$path"
        printf '%s %s\t%s\0' "$mode" "$blob" "$new_path"
    done \
    | GIT_INDEX_FILE="$rename_index" git_repo update-index -z --index-info
move_tree="$(GIT_INDEX_FILE="$rename_index" git_repo write-tree)"
move_commit="$(git_repo commit-tree "$move_tree" -p "$old_commit" -F - <<'MESSAGE'
Move the Python package to python/horsies/

The baseline SQL files move to crates/horsies-core/baseline/. Every file
keeps its bytes; the next commit changes the references, the build and the
SQL reads. This commit alone does not build.

Issues: #682, #889.
MESSAGE
)"
old_count="$(git_repo ls-tree -r --name-only "$old_commit" -- horsies/ | wc -l | tr -d ' ')"
exact_renames="$(git_repo diff -M --name-status "$old_commit" "$move_commit" | grep -c '^R100' || true)"
not_renamed="$(git_repo diff -M --name-status "$old_commit" "$move_commit" | grep -vc '^R100' || true)"
[ "$exact_renames" -eq "$old_count" ] && [ "$not_renamed" -eq 0 ] \
    || fail "the rename commit holds $exact_renames exact renames of $old_count files and $not_renamed other entries"
final_tree="$(git_repo write-tree)"
final_commit="$(git_repo commit-tree "$final_tree" -p "$move_commit" -F - <<'MESSAGE'
Build the Python package with maturin from python/

pyproject.toml builds horsies._core from crates/horsies-python with maturin
(python-source "python", module-name "horsies._core"). Path references in
tests, CI, configs, comments and docs follow the move. The core exports the
baseline SQL files, horsies._core.baseline_sql_files() returns them, and
load_baseline_migration() reads them from there; the SQL bytes and the
migration 1000 checksum do not change.

Issues: #682, #889.
MESSAGE
)"
git_repo update-ref -m 'package move' HEAD "$final_commit" "$old_commit"
git_repo diff --cached --quiet || fail 'the index differs from the new HEAD'
echo "rename commit: $move_commit (every entry an exact rename)"
echo "edit commit: $final_commit"
echo 'Two commits are written on the current branch. Nothing is pushed.'
