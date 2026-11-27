// The page checks: one case per problem.

import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { pageProblems, scriptAssetOf, scriptProblems } from "./web-page.ts";

const PAGE = '<base href="/"><script>window.__HORSIES_UI__ = {}</script><script type="module" src="./assets/app-1a2b.js"></script>';

describe("scriptAssetOf", () => {
  test("named: the first script asset of the page", () => {
    assert.deepEqual(scriptAssetOf(PAGE), { kind: "named", path: "assets/app-1a2b.js" });
  });

  test("notNamed: a page with no script asset", () => {
    assert.deepEqual(scriptAssetOf("<html></html>"), { kind: "notNamed" });
  });
});

describe("pageProblems", () => {
  test("passes a page with status 200, the configuration and a script", () => {
    assert.deepEqual(pageProblems({ status: 200, text: PAGE }), []);
  });

  test("a status that is not 200", () => {
    assert.deepEqual(pageProblems({ status: 503, text: PAGE }), ["GET / answered 503"]);
  });

  test("a page with no injected configuration", () => {
    assert.deepEqual(pageProblems({ status: 200, text: '<script src="assets/app.js"></script>' }), ["the page has no __HORSIES_UI__"]);
  });

  test("a page that names no script asset", () => {
    assert.deepEqual(pageProblems({ status: 200, text: "window.__HORSIES_UI__ = {}" }), ["the page names no script asset"]);
  });
});

describe("scriptProblems", () => {
  test("passes status 200", () => {
    assert.deepEqual(scriptProblems("assets/app.js", { status: 200, text: "" }), []);
  });

  test("a status that is not 200", () => {
    assert.deepEqual(scriptProblems("assets/app.js", { status: 404, text: "" }), ["the script assets/app.js answered 404"]);
  });
});
