//! Deterministic multi-format schema storage and structural payload validation.

use std::collections::{BTreeMap, BTreeSet};

use epoch_core::{EpochError, EpochResult, validate_resource_name};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const MAX_SCHEMAS: usize = 10_000;
const MAX_SCHEMA_REVISIONS: usize = 1_000;
const MAX_SCHEMA_DEFINITION_BYTES: usize = 256 * 1024;
const MAX_SCHEMA_FIELDS: usize = 4_096;
const MAX_SCHEMA_PATH_BYTES: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaFormat {
    Avro,
    JsonSchema,
    Protobuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaCompatibility {
    None,
    Backward,
    Forward,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaValueType {
    Null,
    Boolean,
    Integer,
    Number,
    String,
    Array,
    Object,
}

impl SchemaValueType {
    fn accepts(self, value: &Value) -> bool {
        match self {
            Self::Null => value.is_null(),
            Self::Boolean => value.is_boolean(),
            Self::Integer => value.as_i64().is_some() || value.as_u64().is_some(),
            Self::Number => value.is_number(),
            Self::String => value.is_string(),
            Self::Array => value.is_array(),
            Self::Object => value.is_object(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaField {
    pub path: String,
    pub value_type: SchemaValueType,
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaRegistration {
    pub name: String,
    pub format: SchemaFormat,
    pub definition: String,
    pub compatibility: SchemaCompatibility,
    #[serde(default)]
    pub fields: Vec<SchemaField>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaRevision {
    pub name: String,
    pub revision: u32,
    pub format: SchemaFormat,
    pub definition: String,
    pub compatibility: SchemaCompatibility,
    pub fields: Vec<SchemaField>,
    pub created_at_ms: u64,
}

impl SchemaRevision {
    pub fn reference(&self) -> String {
        format!("{}@{}", self.name, self.revision)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaRegistry {
    schemas: BTreeMap<String, Vec<SchemaRevision>>,
}

impl SchemaRegistry {
    pub fn register(
        &mut self,
        registration: SchemaRegistration,
        created_at_ms: u64,
    ) -> EpochResult<SchemaRevision> {
        validate_registration(&registration)?;
        if !self.schemas.contains_key(&registration.name) && self.schemas.len() >= MAX_SCHEMAS {
            return Err(EpochError::Capacity(format!(
                "schema registry reached its {MAX_SCHEMAS} schema limit"
            )));
        }
        let revisions = self.schemas.entry(registration.name.clone()).or_default();
        if revisions.len() >= MAX_SCHEMA_REVISIONS {
            return Err(EpochError::Capacity(format!(
                "schema {} reached its {MAX_SCHEMA_REVISIONS} revision limit",
                registration.name
            )));
        }
        if let Some(previous) = revisions.last() {
            validate_compatible(previous, &registration)?;
        }
        let revision = u32::try_from(revisions.len())
            .map_err(|_| EpochError::Capacity("schema revision overflow".into()))?
            .checked_add(1)
            .ok_or_else(|| EpochError::Capacity("schema revision overflow".into()))?;
        let stored = SchemaRevision {
            name: registration.name,
            revision,
            format: registration.format,
            definition: registration.definition,
            compatibility: registration.compatibility,
            fields: registration.fields,
            created_at_ms,
        };
        revisions.push(stored.clone());
        Ok(stored)
    }

    pub fn revision(&self, reference: &str) -> EpochResult<&SchemaRevision> {
        let (name, revision) = parse_reference(reference)?;
        self.schemas
            .get(name)
            .and_then(|revisions| revisions.get(revision.saturating_sub(1) as usize))
            .filter(|schema| schema.revision == revision)
            .ok_or_else(|| EpochError::NotFound(reference.to_owned()))
    }

    pub fn latest(&self, name: &str) -> EpochResult<&SchemaRevision> {
        validate_resource_name(name)?;
        self.schemas
            .get(name)
            .and_then(|revisions| revisions.last())
            .ok_or_else(|| EpochError::NotFound(name.to_owned()))
    }

    pub fn validate_payload(&self, reference: &str, payload: &Value) -> EpochResult<()> {
        let schema = self.revision(reference)?;
        for field in &schema.fields {
            match value_at_path(payload, &field.path) {
                Some(value) if field.value_type.accepts(value) => {}
                Some(_) => {
                    return Err(EpochError::InvalidArgument(format!(
                        "schema {reference} field {} must be {:?}",
                        field.path, field.value_type
                    )));
                }
                None if field.required && field.default.is_none() => {
                    return Err(EpochError::InvalidArgument(format!(
                        "schema {reference} required field {} is missing",
                        field.path
                    )));
                }
                None => {}
            }
        }
        Ok(())
    }

    pub fn schema_count(&self) -> usize {
        self.schemas.len()
    }

    pub fn revision_count(&self) -> usize {
        self.schemas.values().map(Vec::len).sum()
    }

    pub(crate) fn validate_snapshot(&self) -> EpochResult<()> {
        if self.schemas.len() > MAX_SCHEMAS {
            return Err(EpochError::InvalidArgument(
                "schema snapshot exceeds the schema limit".into(),
            ));
        }
        for (name, revisions) in &self.schemas {
            validate_resource_name(name)?;
            if revisions.is_empty() || revisions.len() > MAX_SCHEMA_REVISIONS {
                return Err(EpochError::InvalidArgument(format!(
                    "schema {name} snapshot revision history is invalid"
                )));
            }
            for (index, revision) in revisions.iter().enumerate() {
                let expected_revision = u32::try_from(index)
                    .ok()
                    .and_then(|index| index.checked_add(1));
                if revision.name != *name || Some(revision.revision) != expected_revision {
                    return Err(EpochError::InvalidArgument(format!(
                        "schema {name} snapshot revision sequence is invalid"
                    )));
                }
                let registration = SchemaRegistration {
                    name: revision.name.clone(),
                    format: revision.format,
                    definition: revision.definition.clone(),
                    compatibility: revision.compatibility,
                    fields: revision.fields.clone(),
                };
                validate_registration(&registration)?;
                if let Some(previous) = index.checked_sub(1).and_then(|index| revisions.get(index))
                {
                    validate_compatible(previous, &registration)?;
                }
            }
        }
        Ok(())
    }
}

fn validate_registration(registration: &SchemaRegistration) -> EpochResult<()> {
    validate_resource_name(&registration.name)?;
    if registration.definition.is_empty()
        || registration.definition.len() > MAX_SCHEMA_DEFINITION_BYTES
    {
        return Err(EpochError::InvalidArgument(format!(
            "schema definition must be between 1 and {MAX_SCHEMA_DEFINITION_BYTES} bytes"
        )));
    }
    match registration.format {
        SchemaFormat::Avro | SchemaFormat::JsonSchema => {
            let definition: Value =
                serde_json::from_str(&registration.definition).map_err(|error| {
                    EpochError::InvalidArgument(format!("schema definition is not JSON: {error}"))
                })?;
            if !definition.is_object() {
                return Err(EpochError::InvalidArgument(
                    "Avro and JSON Schema definitions must be JSON objects".into(),
                ));
            }
        }
        SchemaFormat::Protobuf => {
            if !registration.definition.contains("message ") {
                return Err(EpochError::InvalidArgument(
                    "Protobuf definitions must declare at least one message".into(),
                ));
            }
        }
    }
    if registration.fields.len() > MAX_SCHEMA_FIELDS {
        return Err(EpochError::InvalidArgument(format!(
            "schema has {} fields; maximum is {MAX_SCHEMA_FIELDS}",
            registration.fields.len()
        )));
    }
    let mut paths = BTreeSet::new();
    for field in &registration.fields {
        validate_path(&field.path)?;
        if !paths.insert(&field.path) {
            return Err(EpochError::InvalidArgument(format!(
                "schema field {} is duplicated",
                field.path
            )));
        }
        if let Some(default) = &field.default
            && !field.value_type.accepts(default)
        {
            return Err(EpochError::InvalidArgument(format!(
                "schema field {} default has the wrong type",
                field.path
            )));
        }
    }
    Ok(())
}

fn validate_compatible(
    previous: &SchemaRevision,
    candidate: &SchemaRegistration,
) -> EpochResult<()> {
    if previous.format != candidate.format {
        return Err(EpochError::Conflict(
            "schema format cannot change between revisions".into(),
        ));
    }
    match candidate.compatibility {
        SchemaCompatibility::None => Ok(()),
        SchemaCompatibility::Backward => backward_compatible(previous, candidate),
        SchemaCompatibility::Forward => forward_compatible(previous, candidate),
        SchemaCompatibility::Full => {
            backward_compatible(previous, candidate)?;
            forward_compatible(previous, candidate)
        }
    }
}

fn backward_compatible(
    previous: &SchemaRevision,
    candidate: &SchemaRegistration,
) -> EpochResult<()> {
    let next = fields_by_path(&candidate.fields);
    for field in previous.fields.iter().filter(|field| field.required) {
        let Some(replacement) = next.get(field.path.as_str()) else {
            return compatibility_error(&field.path, "required field was removed");
        };
        if replacement.value_type != field.value_type {
            return compatibility_error(&field.path, "field type changed");
        }
    }
    for field in candidate.fields.iter().filter(|field| field.required) {
        if !previous.fields.iter().any(|old| old.path == field.path) && field.default.is_none() {
            return compatibility_error(&field.path, "new required field has no default");
        }
    }
    Ok(())
}

fn forward_compatible(
    previous: &SchemaRevision,
    candidate: &SchemaRegistration,
) -> EpochResult<()> {
    let old = fields_by_path(&previous.fields);
    for field in candidate.fields.iter().filter(|field| field.required) {
        let Some(prior) = old.get(field.path.as_str()) else {
            return compatibility_error(
                &field.path,
                "new required field is unknown to old readers",
            );
        };
        if prior.value_type != field.value_type {
            return compatibility_error(&field.path, "field type changed");
        }
    }
    Ok(())
}

fn fields_by_path(fields: &[SchemaField]) -> BTreeMap<&str, &SchemaField> {
    fields
        .iter()
        .map(|field| (field.path.as_str(), field))
        .collect()
}

fn compatibility_error<T>(path: &str, detail: &str) -> EpochResult<T> {
    Err(EpochError::Conflict(format!(
        "schema compatibility failed for {path}: {detail}"
    )))
}

fn parse_reference(reference: &str) -> EpochResult<(&str, u32)> {
    let (name, revision) = reference.rsplit_once('@').ok_or_else(|| {
        EpochError::InvalidArgument("schema reference must use name@revision".into())
    })?;
    validate_resource_name(name)?;
    let revision = revision.parse::<u32>().map_err(|_| {
        EpochError::InvalidArgument("schema reference revision must be an integer".into())
    })?;
    if revision == 0 {
        return Err(EpochError::InvalidArgument(
            "schema reference revision must be non-zero".into(),
        ));
    }
    Ok((name, revision))
}

fn validate_path(path: &str) -> EpochResult<()> {
    if path.is_empty()
        || path.len() > MAX_SCHEMA_PATH_BYTES
        || path.split('.').any(str::is_empty)
        || path.chars().any(char::is_control)
    {
        return Err(EpochError::InvalidArgument(format!(
            "invalid schema field path {path:?}"
        )));
    }
    Ok(())
}

fn value_at_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').try_fold(value, |current, segment| {
        current.as_object().and_then(|object| object.get(segment))
    })
}
