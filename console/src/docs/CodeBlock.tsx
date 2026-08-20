import { useCallback, useEffect, useId, useMemo, useState, type KeyboardEvent, type ReactNode } from "react";

import { languageForLabel, languageName, tokenize, type CodeLanguage } from "../highlight";
import { languages, type LanguageId } from "./content";
import {
  LanguageContext,
  languageStorageKey,
  readStoredLanguage,
  useLanguagePreference,
} from "./languageContext";

export function LanguagePreferenceProvider({ children }: { children: ReactNode }) {
  const [language, setLanguageState] = useState<LanguageId>(readStoredLanguage);

  const setLanguage = useCallback((next: LanguageId) => {
    setLanguageState(next);
    try {
      window.localStorage.setItem(languageStorageKey, next);
    } catch {
      // A blocked storage partition should not break the picker.
    }
  }, []);

  const value = useMemo(() => ({ language, setLanguage }), [language, setLanguage]);
  return <LanguageContext.Provider value={value}>{children}</LanguageContext.Provider>;
}

/* --------------------------------------------------------------------------
   Copy behaviour
   -------------------------------------------------------------------------- */

function useCopyButton(value: string) {
  const [status, setStatus] = useState<"idle" | "copied" | "failed">("idle");

  useEffect(() => {
    if (status === "idle") {
      return;
    }
    const timer = window.setTimeout(() => setStatus("idle"), 2000);
    return () => window.clearTimeout(timer);
  }, [status]);

  const copy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(value);
      setStatus("copied");
    } catch {
      setStatus("failed");
    }
  }, [value]);

  const label = status === "copied" ? "Copied" : status === "failed" ? "Copy failed" : "Copy";
  return { copy, label };
}

/* --------------------------------------------------------------------------
   Highlighted source
   -------------------------------------------------------------------------- */

function Highlighted({ value, language }: { value: string; language: CodeLanguage }) {
  const tokens = useMemo(() => tokenize(value, language), [value, language]);
  return (
    <code>
      {tokens.map((token, index) =>
        token.kind === "plain" ? (
          token.text
        ) : (
          <span key={index} className={`tok-${token.kind}`}>
            {token.text}
          </span>
        ),
      )}
    </code>
  );
}

const previewLines = 16;

interface SourceViewProps {
  value: string;
  language: CodeLanguage;
  collapsible: boolean;
  panelId?: string;
  labelledBy?: string;
}

function SourceView({ value, language, collapsible, panelId, labelledBy }: SourceViewProps) {
  const [expanded, setExpanded] = useState(false);
  const lines = useMemo(() => value.split("\n"), [value]);
  const collapsed = collapsible && !expanded && lines.length > previewLines + 4;
  const shown = collapsed ? lines.slice(0, previewLines).join("\n") : value;

  return (
    <div
      className="code-block__body"
      id={panelId}
      role={panelId ? "tabpanel" : undefined}
      aria-labelledby={labelledBy}
    >
      <pre tabIndex={0} data-collapsed={collapsed || undefined}>
        <Highlighted value={shown} language={language} />
      </pre>
      {collapsible && lines.length > previewLines + 4 ? (
        <button type="button" className="code-block__expand" onClick={() => setExpanded((open) => !open)}>
          {expanded ? "Collapse" : `Show all ${lines.length} lines`}
        </button>
      ) : null}
    </div>
  );
}

/* --------------------------------------------------------------------------
   Single-language block
   -------------------------------------------------------------------------- */

export function CodeBlock({
  label,
  value,
  collapsible = true,
}: {
  label: string;
  value: string;
  collapsible?: boolean;
}) {
  const language = languageForLabel(label);
  const { copy, label: copyLabel } = useCopyButton(value);

  return (
    <div className="code-block">
      <div className="code-block__toolbar">
        <span className="code-block__label">
          <span className="code-block__language">{languageName(language)}</span>
          {label}
        </span>
        <button type="button" onClick={() => void copy()} aria-live="polite">
          {copyLabel}
        </button>
      </div>
      <SourceView value={value} language={language} collapsible={collapsible} />
    </div>
  );
}

/* --------------------------------------------------------------------------
   Multi-language block — tabs live on the block, the way every SDK doc does it
   -------------------------------------------------------------------------- */

export interface CodeSample {
  language: LanguageId;
  filename: string;
  code: string;
}

export function CodeTabs({
  label,
  samples,
  collapsible = true,
}: {
  label: string;
  samples: ReadonlyArray<CodeSample>;
  collapsible?: boolean;
}) {
  const { language, setLanguage } = useLanguagePreference();
  const groupId = useId().replace(/:/g, "");
  const active = samples.find((sample) => sample.language === language) ?? samples[0];
  const { copy, label: copyLabel } = useCopyButton(active?.code ?? "");

  if (!active) {
    return null;
  }

  function handleKey(event: KeyboardEvent<HTMLButtonElement>, current: LanguageId) {
    const index = samples.findIndex((sample) => sample.language === current);
    let next: number | null = null;
    if (event.key === "ArrowRight" || event.key === "ArrowDown") {
      next = (index + 1) % samples.length;
    } else if (event.key === "ArrowLeft" || event.key === "ArrowUp") {
      next = (index - 1 + samples.length) % samples.length;
    } else if (event.key === "Home") {
      next = 0;
    } else if (event.key === "End") {
      next = samples.length - 1;
    }
    if (next === null) {
      return;
    }
    event.preventDefault();
    const target = samples[next];
    if (!target) {
      return;
    }
    setLanguage(target.language);
    window.requestAnimationFrame(() => document.getElementById(`${groupId}-tab-${target.language}`)?.focus());
  }

  return (
    <div className="code-block code-block--tabbed">
      <div className="code-block__toolbar">
        <div className="code-tabs" role="tablist" aria-label={`${label} language`}>
          {samples.map((sample) => {
            const meta = languages.find((candidate) => candidate.id === sample.language);
            return (
              <button
                key={sample.language}
                id={`${groupId}-tab-${sample.language}`}
                type="button"
                role="tab"
                aria-selected={active.language === sample.language}
                aria-controls={`${groupId}-panel-${sample.language}`}
                tabIndex={active.language === sample.language ? 0 : -1}
                onClick={() => setLanguage(sample.language)}
                onKeyDown={(event) => handleKey(event, sample.language)}
              >
                {meta?.label ?? sample.language}
              </button>
            );
          })}
        </div>
        <span className="code-block__filename">{active.filename}</span>
        <button type="button" onClick={() => void copy()} aria-live="polite">
          {copyLabel}
        </button>
      </div>
      <SourceView
        key={active.language}
        value={active.code}
        language={languageForLabel(active.filename)}
        collapsible={collapsible}
        panelId={`${groupId}-panel-${active.language}`}
        labelledBy={`${groupId}-tab-${active.language}`}
      />
    </div>
  );
}
