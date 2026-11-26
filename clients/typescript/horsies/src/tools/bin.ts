#!/usr/bin/env node
// The `horsies` command of the npm package (the `bin` of `package.json`): `get-docs` and `skills install`, for example
// `npx horsies skills install --target .claude/skills`. It loads no native addon, so it runs on a platform with no
// platform package too.

import { processToolDeps, runTool } from "./run.ts";

process.exitCode = runTool(process.argv.slice(2), processToolDeps());
