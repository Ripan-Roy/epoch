/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_EPOCH_API_BASE_URL?: string;
  readonly VITE_DEFAULT_PAGE?: "console" | "docs";
  readonly VITE_DOCS_ONLY?: "true" | "false";
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
