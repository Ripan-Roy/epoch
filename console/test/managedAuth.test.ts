import assert from "node:assert/strict";
import test from "node:test";

import {
  clearManagedToken,
  loadManagedToken,
  saveManagedToken,
  type ManagedTokenStorage,
} from "../src/api/managedAuth.ts";

function memoryStorage(): ManagedTokenStorage {
  const values = new Map<string, string>();
  return {
    getItem: (key) => values.get(key) ?? null,
    setItem: (key, value) => values.set(key, value),
    removeItem: (key) => values.delete(key),
  };
}

test("managed bearer tokens stay session-scoped and validate before storage", () => {
  const storage = memoryStorage();
  assert.equal(loadManagedToken(storage), null);
  saveManagedToken(storage, "epoch-session-token");
  assert.equal(loadManagedToken(storage), "epoch-session-token");
  clearManagedToken(storage);
  assert.equal(loadManagedToken(storage), null);
});

test("managed bearer storage rejects empty, whitespace, and unbounded values", () => {
  const storage = memoryStorage();
  for (const token of ["", " ", "two words", "line\nbreak", "x".repeat(4097)]) {
    assert.throws(() => saveManagedToken(storage, token), /valid bearer token/);
  }
  assert.equal(loadManagedToken(storage), null);
});
