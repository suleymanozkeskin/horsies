# Measurement results

Each file here is one run's evidence: the conditions it ran under, what it
measured, and the verdict those numbers support. They are written by
`uv run python -m tests.perf ... --write-summary` and committed, because a
number cited in a pull request is only as good as the conditions a reader can
still resolve a year later.

These are public repository artifacts. What that requires of their contents:

- **Conditions, then result.** Server version, sample counts, payload sizes,
  batch sizes, server settings, and whether the host was quiet. A verdict with
  no conditions attached tells a later reader nothing they can act on.
- **No internal shorthand.** No phase identifiers, gate names, or references to
  planning documents — those resolve for whoever ran the measurement and for
  nobody else, permanently.
- **No adopter identification.** Workloads here are synthetic; keep it that way.

Raw per-operation samples are not committed. They are large, unreadable, and
compress badly in a repository, so they are written to `raw/` and uploaded as
run artifacts instead.
