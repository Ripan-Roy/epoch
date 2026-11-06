import assert from "node:assert/strict";
import { readdirSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { ESLint } from "eslint";

const require = createRequire(import.meta.url);
const eslintRequire = createRequire(require.resolve("eslint/package.json"));
const parserRequire = createRequire(
  createRequire(require.resolve("typescript-eslint")).resolve("@typescript-eslint/parser"),
);
const consumers = [
  ["eslint", eslintRequire],
  ["@eslint/config-array", createRequire(eslintRequire.resolve("@eslint/config-array"))],
  [
    "@typescript-eslint/typescript-estree",
    createRequire(parserRequire.resolve("@typescript-eslint/typescript-estree")),
  ],
] as const;

for (const [name, consumerRequire] of consumers) {
  test(`${name} resolves patched minimatch and retains its actual matching API`, () => {
    const version: string = consumerRequire("minimatch/package.json").version;
    assert.equal(version, "11.0.1");
    const { Minimatch, minimatch } = consumerRequire("minimatch");
    const matcher = new Minimatch("src/**/*.{ts,tsx}", { dot: true });
    assert.equal(matcher.pattern, "src/**/*.{ts,tsx}");
    assert.equal(matcher.match("src/docs/page.tsx"), true);
    assert.equal(matcher.match("src/.hidden.ts"), true);
    assert.equal(matcher.match("src/docs/page.css"), false);
    assert.equal(matcher.match("src/docs", true), true);
    assert.equal(matcher.match("unrelated", true), false);
    assert.equal(minimatch("src/.hidden.ts", "src/*.ts", { dot: true }), true);
    assert.equal(new Minimatch("!dist/**").match("src/main.ts"), true);
    assert.equal(new Minimatch("!dist/**").match("dist/main.ts"), false);
    assert.equal(new Minimatch("!dist/**", { flipNegate: true }).match("dist/main.ts"), true);
    assert.equal(minimatch("src/main.ts", "src/@(main|index).ts"), true);

    // The fixed optimization bounds expansion before quadratic globpart work.
    // Only exercise this after the version guard; old copies must not allocate
    // the deliberately pathological pattern during a RED-first test run.
    const bounded = new Minimatch(Array(30).fill("**/../a/b").join("/"), {
      optimizationLevel: 2,
      braceExpandMax: 64,
    });
    assert.equal(bounded.set.length, 64);
  });
}

test("the actual ESLint config discovers every source file and preserves ignores", async () => {
  const cwd = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  const eslint = new ESLint({ cwd });
  const results = await eslint.lintFiles(["src/**/*.{ts,tsx}"]);
  const expected = readdirSync(join(cwd, "src"), { recursive: true, withFileTypes: true })
    .filter((entry) => entry.isFile() && /\.(ts|tsx)$/.test(entry.name))
    .map((entry) => join(entry.parentPath, entry.name))
    .sort();
  assert.ok(expected.length > 0);
  assert.deepEqual(results.map((result) => result.filePath).sort(), expected);
  assert.equal(
    results.reduce((total, result) => total + result.errorCount + result.warningCount, 0),
    0,
  );
  assert.equal(await eslint.isPathIgnored(join(cwd, "dist/generated.ts")), true);
  assert.equal(await eslint.isPathIgnored(join(cwd, "node_modules/example/index.ts")), true);
  assert.equal(await eslint.isPathIgnored(join(cwd, "src/main.tsx")), false);
});
