const managedTokenKey = "epoch.control.bearer.v1";
const maxManagedTokenBytes = 4096;

export interface ManagedTokenStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): unknown;
  removeItem(key: string): unknown;
}

export function loadManagedToken(storage: ManagedTokenStorage): string | null {
  const token = storage.getItem(managedTokenKey);
  return token && validManagedToken(token) ? token : null;
}

export function saveManagedToken(storage: ManagedTokenStorage, token: string): void {
  if (!validManagedToken(token)) {
    throw new Error("Enter a valid bearer token without whitespace.");
  }
  storage.setItem(managedTokenKey, token);
}

export function clearManagedToken(storage: ManagedTokenStorage): void {
  storage.removeItem(managedTokenKey);
}

export function loadBrowserManagedToken(): string | null {
  const storage = browserSessionStorage();
  if (!storage) {
    return null;
  }
  try {
    return loadManagedToken(storage);
  } catch {
    return null;
  }
}

export function saveBrowserManagedToken(token: string): void {
  const storage = browserSessionStorage();
  if (!storage) {
    throw new Error("Managed credentials require available browser session storage.");
  }
  saveManagedToken(storage, token);
}

export function clearBrowserManagedToken(): void {
  const storage = browserSessionStorage();
  if (storage) {
    try {
      clearManagedToken(storage);
    } catch {
      // Clearing an already unusable browser store has the same local result.
    }
  }
}

function browserSessionStorage(): ManagedTokenStorage | null {
  if (typeof window === "undefined") {
    return null;
  }
  try {
    return window.sessionStorage;
  } catch {
    return null;
  }
}

function validManagedToken(token: string): boolean {
  if (!token || token.length > maxManagedTokenBytes) {
    return false;
  }
  for (const character of token) {
    const code = character.charCodeAt(0);
    if (code < 0x21 || code > 0x7e) {
      return false;
    }
  }
  return true;
}
