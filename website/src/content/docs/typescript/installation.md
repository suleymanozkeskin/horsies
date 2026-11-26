---
title: Installation
summary: Install the horsies npm package on Node or Bun. The package manager selects one prebuilt native addon for the platform, so no Rust toolchain and no build step are necessary.
related: [index, quick-start/getting-started, configuration/app-config]
tags: [typescript, install, npm, bun, node, platforms, native addon, typescript version]
---

# Installation

The `horsies` package is TypeScript and JavaScript over a native addon that holds the core. The addon is prebuilt for each supported platform and ships in its own npm package. The package manager installs the one platform package that matches the host, so an install needs no Rust toolchain, no compiler and no install script.

## Requirements

| Item | Floor |
|------|-------|
| Node | 26 or later (`engines.node` is `>=26`) |
| Bun | 1.4 or later |
| TypeScript | 6.0 or later, for the published types |
| Platform | One row of [Platforms](#platforms) |
| Database | PostgreSQL 14 or later. A database belongs to one client: the core refuses a database that the Python client or the Rust client installed |

## Install the Package

```bash
npm install horsies
```

```bash
bun add horsies
```

The package is ES modules only. Its entry points are `horsies`, `horsies/zod`, `horsies/rerun/pg` and `horsies/rerun/postgres`. The `exports` map allows no other import path.

## Optional Peer Packages

Install a peer package only for the entry point or the feature that needs it.

| Package | Version range | Needed for |
|---------|---------------|------------|
| `zod` | `>=4.1.13 <4.7.0` | zod schemas in `defineTask` and `defineWorkflow`, and the `hz` codecs of `horsies/zod`. See [Wire Types](/horsies/typescript/tasks/wire-types/) |
| `pg` | `>=8.23.1 <8.24.0` | `horsies/rerun/pg`. See [Rerun](/horsies/typescript/operations/rerun/) |
| `postgres` | `>=3.4.9 <3.5.0` | `horsies/rerun/postgres`. See [Rerun](/horsies/typescript/operations/rerun/) |

The `horsies` entry point imports none of them. An app with `h.*` schemas only needs no peer package.

## Platforms

| Host | Platform package | C library |
|------|------------------|-----------|
| Linux x86_64 | `@horsies/native-linux-x64-gnu` | glibc 2.17 or later |
| Linux arm64 | `@horsies/native-linux-arm64-gnu` | glibc 2.28 or later |
| macOS arm64 | `@horsies/native-darwin-arm64` | - |

Each platform package declares `os`, `cpu` and, on Linux, `libc`, so the package manager installs only the matching one. The packages are optional dependencies of `horsies`.

These hosts have no platform package:

- Linux with musl (for example Alpine Linux). Use a glibc image, for example `node:26-trixie-slim` or `oven/bun:debian`.
- macOS on x86_64.
- Windows.

## A Host With No Platform Package

Importing `horsies` loads no native code. The first call that needs the core loads the addon; `Horsies.create` is such a call, because it checks the config in the core. On a host with no platform package, that call throws `CoreStartFailed` (code `CORE_START_FAILED`) with the message:

```text
the core could not start: no native addon of horsies is built for <host>
```

`<host>` is the platform and the CPU of the process, for example `darwin-x64` or `win32-x64`; on Linux it also names the C library, for example `linux-x64-musl`. When the platform package of the host is supported but missing or does not load, the message names the package and the cause:

```text
the core could not start: the native addon did not load from the package @horsies/native-linux-x64-gnu: <cause>
```

## TypeScript Setup

The published declarations carry `/// <reference lib="esnext.temporal" />`. A project that imports `horsies` gets the Temporal types (`Temporal.Instant`, `Temporal.PlainDate` and the others) with no `lib` setting. TypeScript 6.0 is the first compiler that ships these declarations, so it is the floor.

The package's type tests compile a project with these options and no `lib` setting:

```json
{
  "compilerOptions": {
    "target": "es2025",
    "module": "nodenext",
    "strict": true,
    "noEmit": true
  }
}
```

Use a `module` or `moduleResolution` setting that reads the `exports` map: `nodenext`, `node16` or `bundler`.

## The Main Thread

Only the main thread of a process loads the addon. A call that needs the core in a `worker_threads` isolate that your code starts throws `AddonOutsideMainThread` (code `ADDON_OUTSIDE_MAIN_THREAD`). Call the app from the main thread.

## Next Steps

| Topic | Page |
|-------|------|
| A first app, a worker and a send | [Getting Started](/horsies/typescript/quick-start/getting-started/) |
| `Horsies.create` and the config | [App Config](/horsies/typescript/configuration/app-config/) |
| The commands of an app's entry, and the `horsies` bin | [CLI Reference](/horsies/typescript/cli/) |
