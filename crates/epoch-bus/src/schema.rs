//! Deterministic multi-format schema storage, compatibility, and payload validation.

use std::collections::{BTreeMap, BTreeSet};

use apache_avro::{
    Schema as AvroSchema, schema_compatibility::SchemaCompatibility as AvroCompatibility,
    types::Value as AvroValue,
};
use epoch_core::{EpochError, EpochResult, validate_resource_name};
use prost_reflect::{
    Cardinality, DescriptorPool, DynamicMessage, FieldDescriptor, Kind as ProtobufKind,
    MessageDescriptor, Value as ProtobufValue,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const MAX_SCHEMAS: usize = 10_000;
const MAX_SCHEMA_REVISIONS: usize = 1_000;
const MAX_SCHEMA_DEFINITION_BYTES: usize = 256 * 1024;
const MAX_SCHEMA_FIELDS: usize = 4_096;
const MAX_SCHEMA_PATH_BYTES: usize = 1_024;
const MAX_SCHEMA_ERROR_BYTES: usize = 1_024;
const PROTOBUF_SCHEMA_FILE: &str = "epoch-schema.proto";

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_message: Option<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_message: Option<String>,
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
            root_message: registration.root_message,
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
        validate_official_payload(schema, reference, payload)?;
        // `fields` is the compatibility overlay used by v1 integration
        // snapshots. New registrations derive their contract from the official
        // definition and may leave this legacy overlay empty.
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
                    root_message: revision.root_message.clone(),
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
    validate_official_definition(
        registration.format,
        &registration.definition,
        registration.root_message.as_deref(),
    )?;
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
    validate_official_compatibility(previous, candidate)?;
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

fn validate_official_definition(
    format: SchemaFormat,
    definition: &str,
    root_message: Option<&str>,
) -> EpochResult<()> {
    match format {
        SchemaFormat::JsonSchema => {
            reject_non_protobuf_root_message(root_message)?;
            let schema = parse_json_schema(definition)?;
            reject_external_json_references(&schema)?;
            jsonschema::meta::validate(&schema).map_err(|error| {
                schema_argument_error("JSON Schema", &error.masked().to_string())
            })?;
            jsonschema::options()
                .with_pattern_options(jsonschema::PatternOptions::regex())
                .should_validate_formats(true)
                .build(&schema)
                .map_err(|error| {
                    schema_argument_error("JSON Schema", &error.masked().to_string())
                })?;
            Ok(())
        }
        SchemaFormat::Avro => {
            reject_non_protobuf_root_message(root_message)?;
            AvroSchema::parse_str(definition)
                .map(|_| ())
                .map_err(|error| schema_argument_error("Avro schema", &error.to_string()))
        }
        SchemaFormat::Protobuf => protobuf_root_message(definition, root_message).map(|_| ()),
    }
}

fn validate_official_payload(
    schema: &SchemaRevision,
    reference: &str,
    payload: &Value,
) -> EpochResult<()> {
    match schema.format {
        SchemaFormat::JsonSchema => {
            let definition = parse_json_schema(&schema.definition)?;
            let validator = jsonschema::options()
                .with_pattern_options(jsonschema::PatternOptions::regex())
                .should_validate_formats(true)
                .build(&definition)
                .map_err(|error| {
                    schema_argument_error("JSON Schema", &error.masked().to_string())
                })?;
            validator
                .validate(payload)
                .map_err(|error| schema_rejection(reference, &error.masked().to_string()))
        }
        SchemaFormat::Avro => {
            let definition = AvroSchema::parse_str(&schema.definition)
                .map_err(|error| schema_argument_error("Avro schema", &error.to_string()))?;
            let value = AvroValue::from(payload.clone());
            value.resolve(&definition).map(|_| ()).map_err(|_| {
                let detail = first_missing_avro_field(&schema.definition, payload).map_or_else(
                    || "payload does not conform".to_owned(),
                    |path| format!("required field {path} is missing"),
                );
                schema_rejection(reference, &detail)
            })
        }
        SchemaFormat::Protobuf => {
            let descriptor =
                protobuf_root_message(&schema.definition, schema.root_message.as_deref())?;
            let encoded = serde_json::to_string(payload)
                .map_err(|error| schema_argument_error("Protobuf payload", &error.to_string()))?;
            let mut deserializer = serde_json::Deserializer::from_str(&encoded);
            let message = DynamicMessage::deserialize(descriptor, &mut deserializer)
                .map_err(|error| schema_rejection(reference, &error.to_string()))?;
            deserializer
                .end()
                .map_err(|error| schema_rejection(reference, &error.to_string()))?;
            validate_required_protobuf_fields(reference, &message, "")
        }
    }
}

fn validate_official_compatibility(
    previous: &SchemaRevision,
    candidate: &SchemaRegistration,
) -> EpochResult<()> {
    match candidate.compatibility {
        SchemaCompatibility::None => return Ok(()),
        SchemaCompatibility::Backward
        | SchemaCompatibility::Forward
        | SchemaCompatibility::Full => {}
    }
    match candidate.format {
        SchemaFormat::JsonSchema => {
            let previous = json_field_contract(&parse_json_schema(&previous.definition)?)?;
            let candidate_contract =
                json_field_contract(&parse_json_schema(&candidate.definition)?)?;
            match candidate.compatibility {
                SchemaCompatibility::Backward => {
                    json_backward_compatible(&previous, &candidate_contract)
                }
                SchemaCompatibility::Forward => {
                    json_forward_compatible(&previous, &candidate_contract)
                }
                SchemaCompatibility::Full => {
                    json_backward_compatible(&previous, &candidate_contract)?;
                    json_forward_compatible(&previous, &candidate_contract)
                }
                SchemaCompatibility::None => Ok(()),
            }
        }
        SchemaFormat::Avro => {
            let previous_schema = AvroSchema::parse_str(&previous.definition)
                .map_err(|error| schema_argument_error("Avro schema", &error.to_string()))?;
            let candidate_schema = AvroSchema::parse_str(&candidate.definition)
                .map_err(|error| schema_argument_error("Avro schema", &error.to_string()))?;
            match candidate.compatibility {
                SchemaCompatibility::Backward => avro_can_read(&previous_schema, &candidate_schema),
                SchemaCompatibility::Forward => avro_can_read(&candidate_schema, &previous_schema),
                SchemaCompatibility::Full => {
                    avro_can_read(&previous_schema, &candidate_schema)?;
                    avro_can_read(&candidate_schema, &previous_schema)
                }
                SchemaCompatibility::None => Ok(()),
            }
        }
        SchemaFormat::Protobuf => {
            let previous_message =
                protobuf_root_message(&previous.definition, previous.root_message.as_deref())?;
            let candidate_message =
                protobuf_root_message(&candidate.definition, candidate.root_message.as_deref())?;
            match candidate.compatibility {
                SchemaCompatibility::Backward => {
                    protobuf_compatible(&previous_message, &candidate_message, false)
                }
                SchemaCompatibility::Forward => {
                    protobuf_compatible(&candidate_message, &previous_message, true)
                }
                SchemaCompatibility::Full => {
                    protobuf_compatible(&previous_message, &candidate_message, false)?;
                    protobuf_compatible(&candidate_message, &previous_message, true)
                }
                SchemaCompatibility::None => Ok(()),
            }
        }
    }
}

fn parse_json_schema(definition: &str) -> EpochResult<Value> {
    let schema: Value = serde_json::from_str(definition).map_err(|error| {
        schema_argument_error("JSON Schema", &format!("definition is not JSON: {error}"))
    })?;
    if !schema.is_object() && !schema.is_boolean() {
        return Err(schema_argument_error(
            "JSON Schema",
            "definition must be a JSON object or boolean",
        ));
    }
    Ok(schema)
}

fn reject_external_json_references(value: &Value) -> EpochResult<()> {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                if matches!(key.as_str(), "$ref" | "$dynamicRef")
                    && value
                        .as_str()
                        .is_none_or(|reference| !reference.starts_with('#'))
                {
                    return Err(schema_argument_error(
                        "JSON Schema",
                        "external references are forbidden; bundle definitions and use local fragments",
                    ));
                }
                reject_external_json_references(value)?;
            }
            Ok(())
        }
        Value::Array(values) => {
            for value in values {
                reject_external_json_references(value)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct JsonFieldContract {
    required: bool,
    has_default: bool,
    accepted_types: BTreeSet<String>,
}

fn json_field_contract(schema: &Value) -> EpochResult<BTreeMap<String, JsonFieldContract>> {
    let mut fields = BTreeMap::new();
    collect_json_fields(schema, "", false, &mut fields)?;
    Ok(fields)
}

fn collect_json_fields(
    schema: &Value,
    parent: &str,
    required: bool,
    fields: &mut BTreeMap<String, JsonFieldContract>,
) -> EpochResult<()> {
    let Some(object) = schema.as_object() else {
        return Ok(());
    };
    for keyword in [
        "$ref",
        "$dynamicRef",
        "allOf",
        "anyOf",
        "oneOf",
        "not",
        "if",
        "then",
        "else",
        "dependentSchemas",
    ] {
        if object.contains_key(keyword) {
            return compatibility_error(
                parent,
                &format!("JSON Schema keyword {keyword} cannot be proven compatible"),
            );
        }
    }
    if !parent.is_empty() {
        fields.insert(
            parent.to_owned(),
            JsonFieldContract {
                required,
                has_default: object.contains_key("default"),
                accepted_types: json_schema_types(object.get("type"))?,
            },
        );
    }
    let required_names = object
        .get("required")
        .and_then(Value::as_array)
        .map(|names| {
            names
                .iter()
                .filter_map(Value::as_str)
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    if let Some(properties) = object.get("properties").and_then(Value::as_object) {
        for (name, property) in properties {
            let path = if parent.is_empty() {
                name.clone()
            } else {
                format!("{parent}.{name}")
            };
            collect_json_fields(
                property,
                &path,
                required_names.contains(name.as_str()),
                fields,
            )?;
        }
    }
    Ok(())
}

fn json_schema_types(value: Option<&Value>) -> EpochResult<BTreeSet<String>> {
    let mut types = BTreeSet::new();
    match value {
        None => {}
        Some(Value::String(value)) => {
            types.insert(value.clone());
        }
        Some(Value::Array(values)) => {
            for value in values {
                let value = value.as_str().ok_or_else(|| {
                    schema_argument_error("JSON Schema", "type arrays must contain strings")
                })?;
                types.insert(value.to_owned());
            }
        }
        Some(_) => {
            return Err(schema_argument_error(
                "JSON Schema",
                "type must be a string or string array",
            ));
        }
    }
    Ok(types)
}

fn json_backward_compatible(
    previous: &BTreeMap<String, JsonFieldContract>,
    candidate: &BTreeMap<String, JsonFieldContract>,
) -> EpochResult<()> {
    for (path, old) in previous {
        let Some(next) = candidate.get(path) else {
            return compatibility_error(path, "JSON property was removed");
        };
        ensure_type_superset(path, &next.accepted_types, &old.accepted_types)?;
    }
    for (path, field) in candidate {
        if field.required && !previous.contains_key(path) && !field.has_default {
            return compatibility_error(path, "new required JSON property has no default");
        }
    }
    Ok(())
}

fn json_forward_compatible(
    previous: &BTreeMap<String, JsonFieldContract>,
    candidate: &BTreeMap<String, JsonFieldContract>,
) -> EpochResult<()> {
    for (path, next) in candidate {
        if let Some(old) = previous.get(path) {
            ensure_type_superset(path, &old.accepted_types, &next.accepted_types)?;
        } else if next.required && !next.has_default {
            return compatibility_error(path, "old readers do not know new required JSON property");
        }
    }
    for (path, old) in previous {
        if old.required && !candidate.contains_key(path) {
            return compatibility_error(
                path,
                "new writers removed a property required by old readers",
            );
        }
    }
    Ok(())
}

fn ensure_type_superset(
    path: &str,
    accepted: &BTreeSet<String>,
    prior: &BTreeSet<String>,
) -> EpochResult<()> {
    if accepted.is_empty() || (!prior.is_empty() && accepted.is_superset(prior)) {
        Ok(())
    } else {
        compatibility_error(path, "JSON property type narrowed or changed")
    }
}

fn avro_can_read(writer: &AvroSchema, reader: &AvroSchema) -> EpochResult<()> {
    AvroCompatibility::can_read(writer, reader)
        .map_err(|error| compatibility_error_value("Avro", &error.to_string()))
}

fn first_missing_avro_field(definition: &str, payload: &Value) -> Option<String> {
    let schema = serde_json::from_str::<Value>(definition).ok()?;
    missing_avro_field(&schema, payload, "")
}

fn missing_avro_field(schema: &Value, payload: &Value, parent: &str) -> Option<String> {
    if schema.get("type")?.as_str()? != "record" {
        return None;
    }
    let payload = payload.as_object()?;
    for field in schema.get("fields")?.as_array()? {
        let name = field.get("name")?.as_str()?;
        let path = if parent.is_empty() {
            name.to_owned()
        } else {
            format!("{parent}.{name}")
        };
        let Some(value) = payload.get(name) else {
            if field.get("default").is_none() {
                return Some(path);
            }
            continue;
        };
        if let Some(nested) = field.get("type")
            && nested.get("type").and_then(Value::as_str) == Some("record")
            && let Some(missing) = missing_avro_field(nested, value, &path)
        {
            return Some(missing);
        }
    }
    None
}

fn reject_non_protobuf_root_message(root_message: Option<&str>) -> EpochResult<()> {
    if root_message.is_some() {
        Err(schema_argument_error(
            "schema",
            "root_message is only valid for Protobuf definitions",
        ))
    } else {
        Ok(())
    }
}

fn protobuf_root_message(
    definition: &str,
    requested_root: Option<&str>,
) -> EpochResult<MessageDescriptor> {
    let descriptor = protox_parse::parse(PROTOBUF_SCHEMA_FILE, definition)
        .map_err(|error| schema_argument_error("Protobuf schema", &error.to_string()))?;
    if !descriptor.dependency.is_empty() {
        return Err(schema_argument_error(
            "Protobuf schema",
            "imports are not supported; register one self-contained source definition",
        ));
    }
    if descriptor.message_type.is_empty() {
        return Err(schema_argument_error(
            "Protobuf schema",
            "definition must declare at least one top-level message",
        ));
    }
    let full_name = resolve_protobuf_root_name(&descriptor, requested_root)?;
    let mut pool = DescriptorPool::new();
    pool.add_file_descriptor_proto(descriptor)
        .map_err(|error| schema_argument_error("Protobuf schema", &error.to_string()))?;
    pool.get_message_by_name(&full_name).ok_or_else(|| {
        schema_argument_error("Protobuf schema", "top-level message cannot be resolved")
    })
}

fn resolve_protobuf_root_name(
    descriptor: &prost_reflect::prost_types::FileDescriptorProto,
    requested_root: Option<&str>,
) -> EpochResult<String> {
    let package = descriptor
        .package
        .as_deref()
        .filter(|package| !package.is_empty());
    let names = descriptor
        .message_type
        .iter()
        .filter_map(|message| message.name.as_deref())
        .map(|name| package.map_or_else(|| name.to_owned(), |package| format!("{package}.{name}")))
        .collect::<Vec<_>>();
    match requested_root {
        Some(requested) => {
            let requested = requested.trim_start_matches('.');
            let matches = names
                .iter()
                .filter(|name| {
                    name.as_str() == requested
                        || name
                            .rsplit_once('.')
                            .is_some_and(|(_, short)| short == requested)
                })
                .collect::<Vec<_>>();
            if matches.len() == 1 {
                Ok(matches[0].clone())
            } else {
                Err(schema_argument_error(
                    "Protobuf schema",
                    "root_message must identify exactly one top-level message",
                ))
            }
        }
        None if names.len() == 1 => Ok(names[0].clone()),
        None => Err(schema_argument_error(
            "Protobuf schema",
            "root_message is required when a definition has multiple top-level messages",
        )),
    }
}

fn validate_required_protobuf_fields(
    reference: &str,
    message: &DynamicMessage,
    parent: &str,
) -> EpochResult<()> {
    let descriptor = prost_reflect::ReflectMessage::descriptor(message);
    for field in descriptor.fields() {
        let path = if parent.is_empty() {
            field.name().to_owned()
        } else {
            format!("{parent}.{}", field.name())
        };
        if field.cardinality() == Cardinality::Required && !message.has_field(&field) {
            return Err(schema_rejection(
                reference,
                &format!("required field {path} is missing"),
            ));
        }
        validate_nested_protobuf_value(reference, &path, message.get_field(&field).as_ref())?;
    }
    Ok(())
}

fn validate_nested_protobuf_value(
    reference: &str,
    path: &str,
    value: &ProtobufValue,
) -> EpochResult<()> {
    match value {
        ProtobufValue::Message(message) => {
            validate_required_protobuf_fields(reference, message, path)
        }
        ProtobufValue::List(values) => {
            for (index, value) in values.iter().enumerate() {
                validate_nested_protobuf_value(reference, &format!("{path}[{index}]"), value)?;
            }
            Ok(())
        }
        ProtobufValue::Map(values) => {
            for value in values.values() {
                validate_nested_protobuf_value(reference, path, value)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn protobuf_compatible(
    writer: &MessageDescriptor,
    reader: &MessageDescriptor,
    reader_is_old: bool,
) -> EpochResult<()> {
    let writer_fields = writer
        .fields()
        .map(|field| (field.number(), field))
        .collect::<BTreeMap<_, _>>();
    let reader_fields = reader
        .fields()
        .map(|field| (field.number(), field))
        .collect::<BTreeMap<_, _>>();
    for (number, writer_field) in &writer_fields {
        if let Some(reader_field) = reader_fields.get(number) {
            ensure_protobuf_field_compatible(writer_field, reader_field)?;
        } else if writer_field.cardinality() == Cardinality::Required && reader_is_old {
            return compatibility_error(
                writer_field.name(),
                "new writers removed a field required by old Protobuf readers",
            );
        }
    }
    for (number, reader_field) in &reader_fields {
        if !writer_fields.contains_key(number)
            && reader_field.cardinality() == Cardinality::Required
        {
            return compatibility_error(
                reader_field.name(),
                "Protobuf reader introduced a required field",
            );
        }
    }
    Ok(())
}

fn ensure_protobuf_field_compatible(
    writer: &FieldDescriptor,
    reader: &FieldDescriptor,
) -> EpochResult<()> {
    if writer.name() != reader.name()
        || writer.cardinality() != reader.cardinality()
        || protobuf_kind_name(&writer.kind()) != protobuf_kind_name(&reader.kind())
        || writer
            .containing_oneof()
            .map(|oneof| oneof.name().to_owned())
            != reader
                .containing_oneof()
                .map(|oneof| oneof.name().to_owned())
    {
        return compatibility_error(
            writer.name(),
            "Protobuf field number was reused with a different name, type, cardinality, or oneof",
        );
    }
    Ok(())
}

fn protobuf_kind_name(kind: &ProtobufKind) -> String {
    match kind {
        ProtobufKind::Message(message) => format!("message:{}", message.full_name()),
        ProtobufKind::Enum(enumeration) => format!("enum:{}", enumeration.full_name()),
        other => format!("{other:?}"),
    }
}

fn schema_argument_error(kind: &str, detail: &str) -> EpochError {
    EpochError::InvalidArgument(bounded_schema_message(&format!(
        "{kind} is invalid: {detail}"
    )))
}

fn schema_rejection(reference: &str, detail: &str) -> EpochError {
    EpochError::InvalidArgument(bounded_schema_message(&format!(
        "schema {reference} rejected payload: {detail}"
    )))
}

fn compatibility_error_value(kind: &str, detail: &str) -> EpochError {
    EpochError::Conflict(bounded_schema_message(&format!(
        "schema compatibility failed for {kind}: {detail}"
    )))
}

fn bounded_schema_message(message: &str) -> String {
    if message.len() <= MAX_SCHEMA_ERROR_BYTES {
        return message.to_owned();
    }
    let mut end = MAX_SCHEMA_ERROR_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &message[..end])
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
