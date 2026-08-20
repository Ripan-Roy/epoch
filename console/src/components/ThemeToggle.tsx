import { useEffect, useState } from "react";

import { applyThemeChoice, readThemeChoice, watchSystemTheme, type ThemeChoice } from "../theme";

const options: ReadonlyArray<{ id: ThemeChoice; label: string }> = [
  { id: "system", label: "System" },
  { id: "light", label: "Light" },
  { id: "dark", label: "Dark" },
];

export function ThemeToggle() {
  const [choice, setChoice] = useState<ThemeChoice>(readThemeChoice);

  useEffect(() => {
    applyThemeChoice(choice);
  }, [choice]);

  useEffect(() => {
    if (choice !== "system") {
      return;
    }
    return watchSystemTheme(() => applyThemeChoice("system"));
  }, [choice]);

  return (
    <div className="theme-toggle" role="group" aria-label="Color theme">
      {options.map((option) => (
        <button
          key={option.id}
          type="button"
          aria-pressed={choice === option.id}
          title={`${option.label} theme`}
          onClick={() => setChoice(option.id)}
        >
          <ThemeIcon choice={option.id} />
          <span className="sr-only">{option.label} theme</span>
        </button>
      ))}
    </div>
  );
}

function ThemeIcon({ choice }: { choice: ThemeChoice }) {
  if (choice === "light") {
    return (
      <svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true" focusable="false">
        <circle cx="8" cy="8" r="3.1" fill="currentColor" />
        <g stroke="currentColor" strokeWidth="1.3" strokeLinecap="round">
          <path d="M8 1.4v1.7M8 12.9v1.7M1.4 8h1.7M12.9 8h1.7M3.3 3.3l1.2 1.2M11.5 11.5l1.2 1.2M12.7 3.3l-1.2 1.2M4.5 11.5l-1.2 1.2" />
        </g>
      </svg>
    );
  }
  if (choice === "dark") {
    return (
      <svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true" focusable="false">
        <path
          d="M13.4 9.8A5.9 5.9 0 0 1 6.2 2.6a5.9 5.9 0 1 0 7.2 7.2Z"
          fill="currentColor"
          stroke="currentColor"
          strokeWidth="1.1"
          strokeLinejoin="round"
        />
      </svg>
    );
  }
  return (
    <svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true" focusable="false">
      <rect
        x="1.6"
        y="2.8"
        width="12.8"
        height="8.6"
        rx="1.4"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.3"
      />
      <path d="M5.6 13.6h4.8" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" />
    </svg>
  );
}
