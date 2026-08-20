/**
 * A small dependency-free tokenizer for the four languages the docs ship:
 * shell transcripts, Go, Java, and Python. It classifies enough to make code
 * readable — comments, strings, numbers, keywords, types, and call sites — and
 * deliberately does nothing clever beyond that.
 */

export type CodeLanguage = "shell" | "go" | "java" | "python";

export type TokenKind = "comment" | "string" | "number" | "keyword" | "type" | "func" | "flag" | "plain";

export interface CodeToken {
  kind: TokenKind;
  text: string;
}

/** Rules are tried in order at each position; the first match wins. */
interface Rule {
  kind: TokenKind;
  pattern: string;
}

const shellRules: ReadonlyArray<Rule> = [
  { kind: "comment", pattern: "#[^\\n]*" },
  { kind: "string", pattern: "'(?:[^'\\\\]|\\\\.)*'|\"(?:[^\"\\\\]|\\\\.)*\"" },
  { kind: "flag", pattern: "\\B--?[A-Za-z][\\w-]*" },
  {
    kind: "keyword",
    pattern:
      "\\b(?:cd|curl|git|go|make|cargo|mvn|javac|java|python3|python|pip|npm|export|echo|set|source|sudo|kill|sleep|then|else|fi|do|done|for|while|if)\\b",
  },
  { kind: "type", pattern: "\\b[A-Z][A-Z0-9_]{2,}\\b" },
  { kind: "number", pattern: "\\b\\d[\\d_.]*\\b" },
];

const goRules: ReadonlyArray<Rule> = [
  { kind: "comment", pattern: "//[^\\n]*|/\\*[\\s\\S]*?\\*/" },
  { kind: "string", pattern: "`[^`]*`|\"(?:[^\"\\\\\\n]|\\\\.)*\"|'(?:[^'\\\\\\n]|\\\\.)*'" },
  {
    kind: "keyword",
    pattern:
      "\\b(?:package|import|func|return|if|else|for|range|switch|case|default|break|continue|go|defer|select|type|struct|interface|map|chan|const|var|nil|true|false|fallthrough|goto)\\b",
  },
  {
    kind: "type",
    pattern:
      "\\b(?:string|int|int8|int16|int32|int64|uint|uint8|uint16|uint32|uint64|byte|rune|float32|float64|bool|error|any)\\b",
  },
  { kind: "func", pattern: "\\b[A-Za-z_]\\w*(?=\\()" },
  { kind: "number", pattern: "\\b0[xX][0-9a-fA-F]+\\b|\\b\\d[\\d_.]*\\b" },
];

const javaRules: ReadonlyArray<Rule> = [
  { kind: "comment", pattern: "//[^\\n]*|/\\*[\\s\\S]*?\\*/" },
  { kind: "string", pattern: '"""[\\s\\S]*?"""|"(?:[^"\\\\\\n]|\\\\.)*"|\'(?:[^\'\\\\\\n]|\\\\.)*\'' },
  { kind: "type", pattern: "@\\w+" },
  {
    kind: "keyword",
    pattern:
      "\\b(?:package|import|public|private|protected|static|final|class|interface|enum|record|extends|implements|new|return|if|else|for|while|do|switch|case|default|break|continue|try|catch|finally|throw|throws|this|super|null|true|false|instanceof|synchronized|volatile|abstract)\\b",
  },
  {
    kind: "type",
    pattern:
      "\\b(?:void|int|long|short|byte|char|float|double|boolean|String|List|Map|Set|Optional|var)\\b|\\b[A-Z]\\w*(?=\\s*[<.(\\s])",
  },
  { kind: "func", pattern: "\\b[a-z_]\\w*(?=\\()" },
  { kind: "number", pattern: "\\b0[xX][0-9a-fA-F]+\\b|\\b\\d[\\d_.]*[LlFfDd]?\\b" },
];

const pythonRules: ReadonlyArray<Rule> = [
  { kind: "comment", pattern: "#[^\\n]*" },
  {
    kind: "string",
    pattern:
      "[a-zA-Z]?(?:\"\"\"[\\s\\S]*?\"\"\"|'''[\\s\\S]*?'''|\"(?:[^\"\\\\\\n]|\\\\.)*\"|'(?:[^'\\\\\\n]|\\\\.)*')",
  },
  { kind: "type", pattern: "@[\\w.]+" },
  {
    kind: "keyword",
    pattern:
      "\\b(?:def|class|return|if|elif|else|for|while|in|not|and|or|is|import|from|as|with|try|except|finally|raise|pass|break|continue|lambda|yield|async|await|global|nonlocal|assert|del)\\b",
  },
  { kind: "type", pattern: "\\b(?:None|True|False|self|cls|int|str|float|bool|list|dict|set|bytes)\\b" },
  { kind: "func", pattern: "\\b[A-Za-z_]\\w*(?=\\()" },
  { kind: "number", pattern: "\\b0[xX][0-9a-fA-F]+\\b|\\b\\d[\\d_.]*\\b" },
];

const rulesByLanguage: Record<CodeLanguage, ReadonlyArray<Rule>> = {
  shell: shellRules,
  go: goRules,
  java: javaRules,
  python: pythonRules,
};

const compiled = new Map<CodeLanguage, RegExp>();

function scanner(language: CodeLanguage): RegExp {
  const cached = compiled.get(language);
  if (cached) {
    return cached;
  }
  const rules = rulesByLanguage[language];
  const source = rules.map((rule) => `(${rule.pattern})`).join("|");
  const built = new RegExp(source, "g");
  compiled.set(language, built);
  return built;
}

/** Splits source into classified tokens. Unmatched spans come back as "plain". */
export function tokenize(source: string, language: CodeLanguage): CodeToken[] {
  const rules = rulesByLanguage[language];
  const pattern = scanner(language);
  const tokens: CodeToken[] = [];
  let lastIndex = 0;

  pattern.lastIndex = 0;
  let match = pattern.exec(source);
  while (match !== null) {
    if (match.index > lastIndex) {
      tokens.push({ kind: "plain", text: source.slice(lastIndex, match.index) });
    }
    // Capture group N + 1 corresponds to rule N.
    const groupIndex = match.findIndex((group, index) => index > 0 && group !== undefined);
    const kind = groupIndex > 0 ? rules[groupIndex - 1]!.kind : "plain";
    tokens.push({ kind, text: match[0] });
    lastIndex = match.index + match[0].length;

    // A zero-length match would loop forever; step past it.
    if (match[0].length === 0) {
      pattern.lastIndex += 1;
    }
    match = pattern.exec(source);
  }

  if (lastIndex < source.length) {
    tokens.push({ kind: "plain", text: source.slice(lastIndex) });
  }
  return tokens;
}

/** Picks a language from a code-block label such as "quickstart.go" or "Terminal A". */
export function languageForLabel(label: string): CodeLanguage {
  if (label.endsWith(".go")) {
    return "go";
  }
  if (label.endsWith(".java")) {
    return "java";
  }
  if (label.endsWith(".py")) {
    return "python";
  }
  return "shell";
}

const languageNames: Record<CodeLanguage, string> = {
  shell: "shell",
  go: "go",
  java: "java",
  python: "python",
};

export function languageName(language: CodeLanguage): string {
  return languageNames[language];
}
