// The Node server of the `web` command (`node:http`) with a fake handler on an ephemeral port. On
// Bun these cases run through Bun's `node:http`.

import { describe } from "node:test";

import { serveOnNode } from "../../../horsies/src/web/node-server.ts";
import { serverCases } from "./server-cases.ts";

describe("serveOnNode", () => {
  serverCases(serveOnNode, false);
});
