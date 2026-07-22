import { useState, type FormEvent, type ReactNode } from "react";

import type {
  BusConfig,
  CacheConfig,
  CreateProfile,
  CreateResourceInput,
  QueueConfig,
  ResourceConfig,
  StreamConfig,
} from "../api/types";
import type { ProfileDefinition } from "../profileDefinitions";

interface ProfileCreateCardProps {
  definition: ProfileDefinition;
  connected: boolean;
  onCreate: (input: CreateResourceInput) => Promise<void>;
}

export function ProfileCreateCard({ definition, connected, onCreate }: ProfileCreateCardProps) {
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const prefix = `create-${definition.profile}`;

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const form = event.currentTarget;
    setError(null);
    setPending(true);
    try {
      const formData = new FormData(form);
      const name = readString(formData, "name");
      const config = buildConfig(definition.profile, formData);
      await onCreate({ profile: definition.profile, name, config });
      form.reset();
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "The resource could not be created.");
    } finally {
      setPending(false);
    }
  }

  return (
    <article className="profile-card" data-profile={definition.profile}>
      <header className="profile-card__header">
        <p className="eyebrow">{definition.eyebrow}</p>
        <h3 id={`${prefix}-title`}>{definition.title}</h3>
        <p>{definition.description}</p>
      </header>

      <dl className="guarantee-note">
        <div>
          <dt>Durability setting</dt>
          <dd>{definition.guarantee}</dd>
        </div>
        <div>
          <dt className="sr-only">Limit</dt>
          <dd>{definition.caveat}</dd>
        </div>
      </dl>

      <form aria-labelledby={`${prefix}-title`} onSubmit={(event) => void submit(event)}>
        <fieldset disabled={!connected || pending}>
          <legend className="sr-only">Create {definition.title.toLowerCase()} resource</legend>
          <TextField
            id={`${prefix}-name`}
            label="Resource name"
            name="name"
            placeholder={resourcePlaceholder(definition.profile)}
            hint="Letters, numbers, dots, dashes, and underscores; 128 characters maximum."
          />
          {profileFields(definition.profile, prefix)}
          <button className="button button--primary" type="submit">
            {pending ? "Creating…" : `Create ${profileNoun(definition.profile)}`}
          </button>
        </fieldset>
      </form>

      {!connected ? <p className="form-lock">Connect to a healthy node before creating resources.</p> : null}
      {error ? (
        <p className="form-error" role="alert">
          {error}
        </p>
      ) : null}
    </article>
  );
}

function profileFields(profile: CreateProfile, prefix: string): ReactNode {
  switch (profile) {
    case "cache":
      return (
        <>
          <div className="field-row">
            <NumberField
              id={`${prefix}-max-entries`}
              label="Maximum entries"
              name="max_entries"
              defaultValue={10_000}
              min={1}
            />
            <NumberField
              id={`${prefix}-ttl`}
              label="Default TTL (ms)"
              name="default_ttl_ms"
              placeholder="No expiry"
              min={1}
              optional
            />
          </div>
          <label className="field" htmlFor={`${prefix}-eviction`}>
            <span>Eviction policy</span>
            <select id={`${prefix}-eviction`} name="eviction" defaultValue="no_eviction">
              <option value="no_eviction">No eviction</option>
              <option value="all_keys_lru">All keys · LRU</option>
              <option value="all_keys_lfu">All keys · LFU</option>
              <option value="all_keys_random">All keys · random</option>
              <option value="volatile_lru">Expiring keys · LRU</option>
              <option value="volatile_lfu">Expiring keys · LFU</option>
              <option value="volatile_random">Expiring keys · random</option>
              <option value="volatile_ttl">Expiring keys · nearest TTL</option>
            </select>
          </label>
        </>
      );
    case "stream":
      return (
        <div className="field-row">
          <NumberField
            id={`${prefix}-partitions`}
            label="Partitions"
            name="partitions"
            defaultValue={1}
            min={1}
            max={1_024}
          />
          <NumberField
            id={`${prefix}-record-limit`}
            label="Records / partition"
            name="max_records_per_partition"
            placeholder="Unbounded"
            min={1}
            optional
          />
        </div>
      );
    case "queue":
      return (
        <>
          <div className="field-row">
            <NumberField
              id={`${prefix}-visibility`}
              label="Visibility (ms)"
              name="visibility_timeout_ms"
              defaultValue={30_000}
              min={1}
            />
            <NumberField
              id={`${prefix}-attempts`}
              label="Maximum attempts"
              name="max_attempts"
              defaultValue={8}
              min={1}
            />
          </div>
          <NumberField
            id={`${prefix}-max-messages`}
            label="Maximum messages"
            name="max_messages"
            defaultValue={100_000}
            min={1}
          />
        </>
      );
    case "event_bus":
      return (
        <label className="check-field" htmlFor={`${prefix}-archive`}>
          <input id={`${prefix}-archive`} name="archive" type="checkbox" defaultChecked />
          <span>
            <strong>Keep a local replay archive</strong>
            <small>Archive retention is process-local in this alpha slice.</small>
          </span>
        </label>
      );
  }
}

function TextField({
  id,
  label,
  name,
  placeholder,
  hint,
}: {
  id: string;
  label: string;
  name: string;
  placeholder?: string;
  hint?: string;
}) {
  const hintId = `${id}-hint`;
  return (
    <label className="field" htmlFor={id}>
      <span>{label}</span>
      <input
        id={id}
        name={name}
        type="text"
        placeholder={placeholder}
        required
        maxLength={128}
        pattern="[A-Za-z0-9._-]+"
        autoComplete="off"
        aria-describedby={hint ? hintId : undefined}
      />
      {hint ? <small id={hintId}>{hint}</small> : null}
    </label>
  );
}

function NumberField({
  id,
  label,
  name,
  defaultValue,
  placeholder,
  min,
  max,
  optional = false,
}: {
  id: string;
  label: string;
  name: string;
  defaultValue?: number;
  placeholder?: string;
  min: number;
  max?: number;
  optional?: boolean;
}) {
  return (
    <label className="field" htmlFor={id}>
      <span>
        {label} {optional ? <em>optional</em> : null}
      </span>
      <input
        id={id}
        name={name}
        type="number"
        inputMode="numeric"
        defaultValue={defaultValue}
        placeholder={placeholder}
        min={min}
        max={max}
        step={1}
        required={!optional}
      />
    </label>
  );
}

function buildConfig(profile: CreateProfile, formData: FormData): ResourceConfig {
  switch (profile) {
    case "cache":
      return {
        max_entries: readPositiveInteger(formData, "max_entries"),
        default_ttl_ms: readOptionalPositiveInteger(formData, "default_ttl_ms"),
        durability: "volatile",
        eviction: readString(formData, "eviction") as CacheConfig["eviction"],
      } satisfies CacheConfig;
    case "stream":
      return {
        partitions: readPositiveInteger(formData, "partitions"),
        durability: "volatile",
        max_records_per_partition: readOptionalPositiveInteger(formData, "max_records_per_partition"),
      } satisfies StreamConfig;
    case "queue":
      return {
        durability: "volatile",
        visibility_timeout_ms: readPositiveInteger(formData, "visibility_timeout_ms"),
        max_messages: readPositiveInteger(formData, "max_messages"),
        retry: {
          strategy: "exponential",
          initial_delay_ms: 1_000,
          max_delay_ms: 60_000,
          jitter_percent: 10,
          max_attempts: readPositiveInteger(formData, "max_attempts"),
          max_age_ms: null,
        },
        dedupe_window_ms: null,
      } satisfies QueueConfig;
    case "event_bus":
      return {
        durability: "volatile",
        archive: formData.get("archive") === "on",
      } satisfies BusConfig;
  }
}

function readString(formData: FormData, name: string): string {
  const value = formData.get(name);
  if (typeof value !== "string" || value.trim() === "") {
    throw new Error(`${humanize(name)} is required.`);
  }
  return value.trim();
}

function readPositiveInteger(formData: FormData, name: string): number {
  const value = Number(readString(formData, name));
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new Error(`${humanize(name)} must be a positive whole number.`);
  }
  return value;
}

function readOptionalPositiveInteger(formData: FormData, name: string): number | null {
  const raw = formData.get(name);
  if (raw === null || raw === "") {
    return null;
  }
  return readPositiveInteger(formData, name);
}

function resourcePlaceholder(profile: CreateProfile): string {
  switch (profile) {
    case "cache":
      return "sessions";
    case "stream":
      return "orders";
    case "queue":
      return "fulfillment";
    case "event_bus":
      return "domain-events";
  }
}

function profileNoun(profile: CreateProfile): string {
  return profile === "event_bus" ? "bus" : profile;
}

function humanize(value: string): string {
  return value.replaceAll("_", " ");
}
