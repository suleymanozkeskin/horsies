# Documentation Standard

Guide for writing horsies documentation. Agents must follow these rules.

---

## Philosophy

- **Concise over verbose.** Say it once, say it clearly.
- **No fluff.** No "simply", "just", "easily", "of course".
- **Agent-friendly.** Structure for machine parsing, not prose enjoyment.
- **One concept per page.** If a page covers two things, split it.
- **Sections exist when useful.** Never force empty sections.

---

## Page Structure

Use these sections. Include only what applies.

### Frontmatter (required)

```yaml
---
title: Short Title
summary: One sentence. What this page covers.
related: [other-page, another-page]
tags: [task, retry, configuration]
---
```

### Title

```markdown
# {Title from frontmatter}
```

### Summary

One paragraph. What this is and why it exists. No "In this document we will...".

### Why and When

When to use this. What problems it solves. Keep it brief—2-5 sentences max.

Omit if the concept is obvious (e.g., "defining tasks" doesn't need a "why").

### How To

Step-by-step or code examples. This is the core of most pages.

Rules:

- Code examples must be self-contained and runnable
- Show imports explicitly
- Use realistic variable names, not `foo`/`bar`
- One example per use case, not one mega-example

### Things to Avoid

Anti-patterns, common mistakes, footguns. Use this format:

~~~markdown
## Things to Avoid

**Don't do X because Y.**

```python
# Wrong
bad_code()

# Correct
good_code()
```
~~~

Omit if there are no meaningful anti-patterns for this topic.

### API Reference

Type signatures, parameters, return values. Dry and precise.

```markdown
## API Reference

### `function_name(param: Type) -> ReturnType`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `param` | `str` | Yes | What it does |

**Returns:** `ReturnType` — description

**Raises:** `ErrorType` — when this happens
```

Omit if the page is purely conceptual (e.g., architecture overview).

---

## Writing Rules

### Do

- Use imperative mood: "Configure the broker" not "You should configure"
- Be specific: "Returns `None` on timeout" not "Returns nothing if it times out"
- Use tables for structured data
- Use code blocks for anything code-related
- Link to related pages when referencing other concepts
- Use `monospace` for code identifiers in prose

### Don't

- Don't explain what the reader already knows
- Don't use hedging language ("might", "could", "perhaps")
- Don't repeat information from other pages—link instead
- Don't use first person ("I", "we")
- Don't add commentary ("Note that...", "It's worth mentioning...")
- Don't use emoji
- NEVER WRITE ANYTHING THAT DOES NOT EXIST IN THE LIBRARY!

### Code Examples

```python
# Every example must:
# 1. Include necessary imports
# 2. Be copy-pasteable
# 3. Use realistic names
# 4. Show the minimal code needed

from horsies import Horsies, TaskResult, TaskError

app = Horsies(config)

@app.task("send_email")
def send_email(*, to: str, subject: str) -> TaskResult[None, TaskError]:
    # Implementation
    return TaskResult(ok=None)
```

### Tone by Section

| Section         | Tone                    |
| --------------- | ----------------------- |
| Summary         | Neutral, informative    |
| Why and When    | Direct, practical       |
| How To          | Imperative, instructional |
| Things to Avoid | Direct, warning         |
| API Reference   | Dry, precise            |

---

## Anti-patterns

**Don't write tutorials disguised as documentation.**
Tutorials guide a beginner through a journey. Documentation describes how things work. Keep them separate.

**Don't scatter the same information across pages.**
If retry policy is explained in `retry-policy.md`, other pages link to it—they don't re-explain.

**Don't write for a specific reader's mood.**
No "Congratulations!", no "Don't worry", no assumptions about frustration or excitement.

**Don't use vague quantifiers.**
"Fast" means nothing. "Processes 10k tasks/second" means something.

**Don't document implementation details that may change.**
Document behavior and contracts, not internal mechanics (unless it's in `/internals`).

---

## File Naming

- Lowercase with hyphens: `retry-policy.md`, not `RetryPolicy.md`
- Descriptive: `defining-tasks.md`, not `tasks.md`
- No version numbers in filenames

## Directory Structure

```text
docs/
├── index.md              # Entry point
├── getting-started.md    # Quickstart (welcoming, not documentation)
├── concepts/             # Mental models, architecture
├── tasks/                # Task-related docs
├── workers/              # Worker-related docs
├── configuration/        # Config options
├── scheduling/           # Scheduler docs
├── internals/            # Implementation details (for contributors)
└── DOCUMENTATION_STANDARD.md  # This file
```

---

## Python Code Blocks Are Type-Checked

CI type-checks every `python` block of the docs and the skill files.

- The blocks of one page share names: a block can use the names that the blocks above it define.
- `from prelude import *` is added to each page. It gives `app`, `my_task`, `broker`, `handle`, `spec`, `ctx` and common standard-library names. The stand-ins are in `tests/docs_examples/stubs/`.
- A block that must not be checked has a marker on the line directly above the fence, with a reason:
  - `.md`: `<!-- check: skip KIND -- reason -->`
  - `.mdx`: `{/* check: skip KIND -- reason */}`
- `KIND` is required:
  - `elided`: the block shortens on purpose (pseudo-code, a signature, a deliberate wrong form, a library definition).
  - `partial`: the block is correct but needs context that the page does not show.
  - `defect`: the block is wrong for the current API. Fix it. A `defect` skip is a known error, not a final state.
- `check: alone` (same form, no reason) checks a block on its own, with no names from other blocks.
- A block that does not parse needs `skip`. Never use `elided` or `partial` for a block that is wrong for the current API.
- Pass `SecretStr(...)` to `PostgresConfig(database_url=...)`.
- Run the check: `uv run python tests/docs_examples/extract.py build/doc_examples && uv run pyright -p pyrightconfig.docs.json`

---

## Checklist Before Committing

- [ ] Frontmatter present with title, summary, related, tags
- [ ] No empty sections
- [ ] Code examples are self-contained
- [ ] No repeated information from other pages
- [ ] Links to related pages work
- [ ] No fluff words
