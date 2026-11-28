---
title: Installation
summary: Install the horsies npm package on Node or Bun. The package manager selects one prebuilt native addon for the platform, so no Rust toolchain and no build step are necessary.
related: [index, quick-start/getting-started, configuration/app-config]
tags: [typescript, install, npm, bun, node, platforms, native addon, typescript version, bun build --compile]
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
| `@opentelemetry/api` | `>=1.7.0 <2` | Spans and producer metrics of an app that turns on OpenTelemetry tracing |

The `horsies` entry point imports none of them. It loads `@opentelemetry/api` only for an app that turns on tracing. An app with `h.*` schemas only needs no peer package.

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

Importing `horsies` loads no native code. The first call that needs the core loads the addon; `Horsies.create` is such a call, because it checks the config in the core. On a host with no platform package, that call throws `RuntimeStartFailed` (code `RUNTIME_START_FAILED`) with the message:

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

## A Single-File Program With `bun build --compile`

`bun build --compile` puts the addon of the installed platform package into the compiled file. The compiled file runs on a host that has no `node_modules` folder.

1. On a host of the target platform, install `horsies` with Bun.
2. Compile the entry file of your program:

   ```bash
   bun build --compile ./src/main.ts --outfile my-app
   ```

3. Copy `my-app` to the target host and run it. The target host needs no Bun, no Node and no `node_modules`.

Build on a host of the target platform. The compiled file holds only the platform package that is installed, and the package manager installs only the package of the build host. A compiled file that holds no platform package for its host fails at the first call that needs the core, with the `RuntimeStartFailed` message of [A Host With No Platform Package](#a-host-with-no-platform-package).

On Bun, the package opens its platform package with a `require` of the package name, which the Bun bundler follows. On Node, the package finds the platform package at run time. Thus a Node single executable application, or a bundle of another bundler, does not hold the addon. For such a program, install the platform package in a `node_modules` folder that the program file can resolve.

## The Main Thread

Only the main thread of a process loads the addon. A call that needs the core in a `worker_threads` isolate that your code starts throws `AddonOutsideMainThread` (code `ADDON_OUTSIDE_MAIN_THREAD`). Call the app from the main thread.

## Next Steps

| Topic | Page |
|-------|------|
| A first app, a worker and a send | [Getting Started](/horsies/typescript/quick-start/getting-started/) |
| `Horsies.create` and the config | [App Config](/horsies/typescript/configuration/app-config/) |
| The commands of an app's entry, and the `horsies` bin | [CLI Reference](/horsies/typescript/cli/) |
