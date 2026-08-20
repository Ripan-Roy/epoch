export type ThemeChoice = "system" | "light" | "dark";
export type ResolvedTheme = "light" | "dark";

const storageKey = "epoch-theme";
const choices: ReadonlyArray<ThemeChoice> = ["system", "light", "dark"];

/** Reads the stored preference, falling back to following the operating system. */
export function readThemeChoice(): ThemeChoice {
  try {
    const stored = window.localStorage.getItem(storageKey);
    return choices.includes(stored as ThemeChoice) ? (stored as ThemeChoice) : "system";
  } catch {
    return "system";
  }
}

export function resolveTheme(choice: ThemeChoice): ResolvedTheme {
  if (choice !== "system") {
    return choice;
  }
  return window.matchMedia?.("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

/** Writes the preference to the document element so CSS can react, and persists it. */
export function applyThemeChoice(choice: ThemeChoice): ResolvedTheme {
  const resolved = resolveTheme(choice);
  document.documentElement.dataset.theme = resolved;
  document.documentElement.dataset.themeChoice = choice;
  try {
    if (choice === "system") {
      window.localStorage.removeItem(storageKey);
    } else {
      window.localStorage.setItem(storageKey, choice);
    }
  } catch {
    // A blocked storage partition should never stop the theme from applying.
  }
  return resolved;
}

/** Calls back whenever the system theme changes, so "system" keeps tracking it. */
export function watchSystemTheme(onChange: () => void): () => void {
  const query = window.matchMedia?.("(prefers-color-scheme: dark)");
  if (!query) {
    return () => {};
  }
  query.addEventListener("change", onChange);
  return () => query.removeEventListener("change", onChange);
}
