import { useEffect, useRef, useState } from "react";

import { LanguagePreferenceProvider } from "./docs/CodeBlock";
import { docsGroups, docsPages, editPageUrl, findDocsPage, type DocsPageMeta } from "./docs/registry";

interface DocsPageProps {
  section: string | null;
  heading?: string | null;
}

export function DocsPage({ section, heading = null }: DocsPageProps) {
  const page = findDocsPage(section);
  const [activeHeading, setActiveHeading] = useState<string | null>(page.headings[0]?.id ?? null);
  const [navigationOpen, setNavigationOpen] = useState(false);
  const navigationButton = useRef<HTMLButtonElement>(null);
  const articleTop = useRef<HTMLDivElement>(null);

  const index = docsPages.findIndex((candidate) => candidate.id === page.id);
  const previous = index > 0 ? docsPages[index - 1] : undefined;
  const next = index >= 0 && index < docsPages.length - 1 ? docsPages[index + 1] : undefined;

  // Landing on a page starts it at the top; a heading in the route jumps to it.
  useEffect(() => {
    setNavigationOpen(false);
    window.requestAnimationFrame(() => {
      if (heading) {
        const target = document.getElementById(heading);
        if (target) {
          target.scrollIntoView();
          setActiveHeading(heading);
          return;
        }
      }
      window.scrollTo({ top: 0 });
      articleTop.current?.focus({ preventScroll: true });
    });
  }, [page.id, heading]);

  useEffect(() => {
    const targets = page.headings
      .map(({ id }) => document.getElementById(id))
      .filter((candidate): candidate is HTMLElement => candidate !== null);
    if (!("IntersectionObserver" in window) || targets.length === 0) {
      return;
    }

    const observer = new IntersectionObserver(
      (entries) => {
        const visible = entries
          .filter((entry) => entry.isIntersecting)
          .sort(
            (left, right) => Math.abs(left.boundingClientRect.top) - Math.abs(right.boundingClientRect.top),
          );
        const current = visible[0]?.target.id;
        if (current) {
          setActiveHeading(current);
        }
      },
      { rootMargin: "-15% 0px -70% 0px", threshold: 0 },
    );

    targets.forEach((target) => observer.observe(target));
    return () => observer.disconnect();
  }, [page.id, page.headings]);

  useEffect(() => {
    if (!navigationOpen) {
      return;
    }
    const closeOnEscape = (event: globalThis.KeyboardEvent) => {
      if (event.key !== "Escape") {
        return;
      }
      setNavigationOpen(false);
      navigationButton.current?.focus();
    };
    window.addEventListener("keydown", closeOnEscape);
    return () => window.removeEventListener("keydown", closeOnEscape);
  }, [navigationOpen]);

  const Body = page.Body;

  return (
    <LanguagePreferenceProvider>
      <main id="main-content" className="docs-main" tabIndex={-1}>
        <div className="docs-shell">
          <div className="docs-mobile-navigation">
            <button
              ref={navigationButton}
              type="button"
              aria-expanded={navigationOpen}
              aria-controls="mobile-docs-navigation"
              onClick={() => setNavigationOpen((open) => !open)}
            >
              <span className="docs-mobile-navigation__icon" aria-hidden="true">
                <i />
                <i />
                <i />
              </span>
              <span>
                <small>{page.group}</small>
                {page.label}
              </span>
              <span aria-hidden="true">{navigationOpen ? "×" : "⌄"}</span>
            </button>
            {navigationOpen ? <DocsNavigation id="mobile-docs-navigation" activePage={page} /> : null}
          </div>

          <div className="docs-layout">
            <aside className="docs-sidebar" aria-label="Documentation navigation">
              <DocsNavigation activePage={page} />
              <div className="docs-sidebar__status">
                <span className="status-dot" data-tone="good" aria-hidden="true" />
                <span>
                  <strong>Private beta</strong>
                  APIs are provisional
                </span>
              </div>
            </aside>

            <article className="docs-article">
              <div ref={articleTop} tabIndex={-1} className="docs-article__anchor">
                <nav className="docs-breadcrumb" aria-label="Breadcrumb">
                  <a href="#/docs">Docs</a>
                  <span aria-hidden="true">/</span>
                  <span>{page.group}</span>
                  <span aria-hidden="true">/</span>
                  <span aria-current="page">{page.label}</span>
                </nav>
                <h1>{page.title}</h1>
                <p className="docs-article__lede">{page.summary}</p>
              </div>

              <Body />

              <nav className="docs-pagination" aria-label="Documentation pages">
                {previous ? (
                  <a href={`#/docs/${previous.id}`} data-direction="previous">
                    <small>Previous</small>
                    <span>{previous.label}</span>
                  </a>
                ) : (
                  <span />
                )}
                {next ? (
                  <a href={`#/docs/${next.id}`} data-direction="next">
                    <small>Next</small>
                    <span>{next.label}</span>
                  </a>
                ) : (
                  <span />
                )}
              </nav>
            </article>

            <aside className="docs-toc">
              <nav aria-label="On this page">
                <p>On this page</p>
                {page.headings.map((item) => (
                  <a
                    key={item.id}
                    href={`#/docs/${page.id}/${item.id}`}
                    aria-current={activeHeading === item.id ? "location" : undefined}
                  >
                    {item.label}
                  </a>
                ))}
              </nav>
              <a className="docs-toc__edit" href={editPageUrl} target="_blank" rel="noreferrer">
                Edit this page <span aria-hidden="true">↗</span>
              </a>
            </aside>
          </div>
        </div>
      </main>
    </LanguagePreferenceProvider>
  );
}

function DocsNavigation({ id, activePage }: { id?: string; activePage: DocsPageMeta }) {
  return (
    <nav id={id} className="docs-navigation" aria-label="Documentation sections">
      {docsGroups.map((group) => (
        <div key={group.label} className="docs-navigation__group">
          <p>{group.label}</p>
          {group.pages.map((item) => (
            <a
              key={item.id}
              href={`#/docs/${item.id}`}
              aria-current={activePage.id === item.id ? "page" : undefined}
            >
              {item.label}
            </a>
          ))}
        </div>
      ))}
    </nav>
  );
}
