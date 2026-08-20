import { createContext, useContext } from "react";

import { languages, type LanguageId } from "./content";

export const languageStorageKey = "epoch-docs-language";

export interface LanguagePreference {
  language: LanguageId;
  setLanguage: (language: LanguageId) => void;
}

export const LanguageContext = createContext<LanguagePreference>({
  language: "go",
  setLanguage: () => {},
});

export function readStoredLanguage(): LanguageId {
  try {
    const stored = window.localStorage.getItem(languageStorageKey);
    return languages.some((candidate) => candidate.id === stored) ? (stored as LanguageId) : "go";
  } catch {
    return "go";
  }
}

/** The language chosen once in any sample, honoured by every sample on every page. */
export function useLanguagePreference(): LanguagePreference {
  return useContext(LanguageContext);
}
