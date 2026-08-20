import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import "@fontsource-variable/ibm-plex-sans/wght.css";
import "@fontsource/ibm-plex-mono/400.css";
import "@fontsource/ibm-plex-mono/500.css";
import "@fontsource/ibm-plex-mono/600.css";

import App from "./App";
import "./styles.css";
import "./docs.css";

const root = document.getElementById("root");

if (!root) {
  throw new Error("Epoch console root element is missing");
}

createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
