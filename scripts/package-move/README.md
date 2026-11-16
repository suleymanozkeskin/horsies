# Package move

`scripts/package-move.sh` moves the Python package to `python/horsies/` and the baseline SQL files into the core
crate, in one step, and checks the result. Issues #682 (package location, maturin) and #889 (SQL files in the core
crate).

## Run

```sh
git clone --branch program/0.8.0 https://github.com/suleymanozkeskin/horsies.git move
scripts/package-move.sh move            # stage the move and check it
scripts/package-move.sh --commit move   # the same, then write two commits
git -C move diff --cached -M --stat
```

The argument is a clean checkout of `program/0.8.0`. Run the script from a checkout of this branch; it reads its
helper files next to itself. It needs `git` and `uv` (uv supplies Python 3.13 and PyYAML). It changes files only
in the target checkout and stages every change there. It never pushes and never builds.

With `--commit`, after every check passes, it writes two commits on the current branch: first the renames with
unchanged bytes (every entry an exact rename), then the edits. In one combined commit, git does not detect
`python/horsies/core/baseline/__init__.py` and `migration.py` as renames, because the edits change most of
their lines; with the two commits, `git log --follow` reaches the history of every moved file. The first commit
alone does not build.

## Steps

1. Preflight: the checkout is clean, the package is at `horsies/`, `pyproject.toml` builds with setuptools, and
   `python/` and `crates/horsies-core/baseline/` do not exist.
2. `git mv horsies/core/baseline/sql crates/horsies-core/baseline`.
3. `git mv horsies python/horsies`.
4. Path references (`package-move/move_rules.py references`):
   - exact edits, each anchor found exactly once: `.gitignore`, `.github/workflows/tests.yml`,
     `pyrightconfig.docs.json`, the tests that build paths from the repository root, `website/` pages that tell
     where a source checkout keeps a file. In `tests.yml`, the Python jobs and the showcase job also run for a
     change to the crates that build `horsies._core` (`crates/horsies-python/`, `crates/horsies-core/`,
     `crates/horsies-macros/`) and to the root Cargo and toolchain files;
   - a rule over every tracked text file: a path that starts at `horsies/` and continues into a package folder
     (`core/`, `monitoring/`, `web/`, `docs/`, `.agents/`) gets the prefix `python/`; `horsies/core/baseline/sql/`
     becomes `crates/horsies-core/baseline/`. A Cargo feature such as `horsies/web` does not match.
5. `pyproject.toml` (`move_rules.py pyproject`): build backend `maturin>=1.16,<2`; `[tool.maturin]` with
   `manifest-path`, `module-name = "horsies._core"`, `python-source = "python"`, `features = ["extension-module"]`
   and `include` for the built web UI, the skill files and the SQL files (sdist); `project.license-files`;
   `tool.pyright.extraPaths = ["python"]`; `tool.uv.cache-keys` for the Rust sources and the files compiled into
   `horsies._core`.
6. `package-move/sql-through-core.patch`: the core exports the baseline files (`BASELINE_SQL_FILE_TEXTS`),
   `horsies._core.baseline_sql_files()` returns them, and `load_baseline_migration()` reads them from there. The
   SQL bytes do not change, so the migration 1000 checksum does not change.
7. Checks (`move_rules.py check`, then a YAML parse). Any failure stops the script with a message.
8. With `--commit`: the two commits.

## Checks

- Every file of the old `horsies/` is at its new path, and no tracked file or folder is left at the old path.
- Each SQL file in `crates/horsies-core/baseline/` has the same git blob as before the move.
- No tracked text file cites an old path, except the kept text below.
- Every tracked TOML file, every tracked YAML file and both pyright configs parse.
- `pyproject.toml` builds with maturin, its `manifest-path` is a crate whose `[lib] name` is `_core` and that has
  the feature `extension-module`, and its `include` entries match files.
- No change is left unstaged and no untracked file is left.

## Kept text

| Text | Why |
|---|---|
| `CHANGELOG.md`, `website/src/content/docs/changelog.md` | Release history |
| `crates/horsies/` | The frozen Rust client; CI refuses a change there |
| Comments in `crates/horsies-core/baseline/*.sql` | Migration 1000; a changed byte changes its checksum |
| `horsies/web/static/…` and `horsies/.agents/skills/horsies/…`: 8 lines of `release.yml`, 1 line of `skills_installer.py`, 6 docs lines | Paths inside the wheel or the installed package, where the package is still `horsies/` |

## When the patch does not apply

`program/0.8.0` moves on. When an edit anchor or `sql-through-core.patch` no longer matches, the script stops before
the checks. Run steps 2 to 5 by hand on a fresh checkout, make the SQL change again, and write the patch with
`git diff > scripts/package-move/sql-through-core.patch` (the working tree against the staged steps 2 to 5).

## After the move

- `import horsies` needs `horsies._core`: the package root imports the broker, the broker imports the baseline
  loader, and the loader imports `horsies._core`. Without the built module, every import of the package fails
  with `ModuleNotFoundError: No module named 'horsies._core'`.
- The first build (`maturin develop` or `uv sync`) compiles `horsies-core` and `horsies-python`. Every `uv sync`
  and `uv run` after the move builds the extension, so a contributor needs the Rust toolchain of
  `rust-toolchain.toml`.
- `horsies-codegen write python/horsies` writes `_core.pyi`. Until it exists, pyright cannot resolve
  `horsies._core`.
- CI still installs with `uv sync`, which now builds the extension in each Python job; the dev-wheel job of #682
  replaces that. `release.yml` still runs `uv build` on the runner, which gives one wheel linked against the
  runner's glibc; the release wheels come from the maturin-action build instead.
