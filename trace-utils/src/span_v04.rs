use crate::no_alloc_string::NoAllocString;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

#[derive(Debug, PartialEq)]
pub enum SpanKey {
    Service,
    Name,
    Resource,
    TraceId,
    SpanId,
    ParentId,
    Start,
    Duration,
    Error,
    Meta,
    Metrics,
    Type,
    MetaStruct,
    SpanLinks,
}

impl FromStr for SpanKey {
    type Err = SpanKeyParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "service" => Ok(SpanKey::Service),
            "name" => Ok(SpanKey::Name),
            "resource" => Ok(SpanKey::Resource),
            "trace_id" => Ok(SpanKey::TraceId),
            "span_id" => Ok(SpanKey::SpanId),
            "parent_id" => Ok(SpanKey::ParentId),
            "start" => Ok(SpanKey::Start),
            "duration" => Ok(SpanKey::Duration),
            "error" => Ok(SpanKey::Error),
            "meta" => Ok(SpanKey::Meta),
            "metrics" => Ok(SpanKey::Metrics),
            "type" => Ok(SpanKey::Type),
            "meta_struct" => Ok(SpanKey::MetaStruct),
            "span_links" => Ok(SpanKey::SpanLinks),
            _ => Err(SpanKeyParseError::from(format!("Invalid span key: {}", s))),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Span {
    pub service: NoAllocString,
    pub name: NoAllocString,
    pub resource: NoAllocString,
    pub r#type: NoAllocString,
    pub trace_id: u64,
    pub span_id: u64,
    pub parent_id: u64,
    pub start: i64,
    pub duration: i64,
    pub error: i32,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub meta: HashMap<NoAllocString, NoAllocString>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub metrics: HashMap<NoAllocString, f64>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub meta_struct: HashMap<NoAllocString, Vec<u8>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub span_links: Vec<SpanLink>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct SpanLink {
    pub trace_id: u64,
    pub trace_id_high: u64,
    pub span_id: u64,
    pub attributes: HashMap<NoAllocString, NoAllocString>,
    pub tracestate: NoAllocString,
    pub flags: u64,
}

#[derive(Debug)]
pub struct SpanKeyParseError {
    details: String,
}

impl fmt::Display for SpanKeyParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SpanKeyParseError: {}", self.details)
    }
}

impl std::error::Error for SpanKeyParseError {}

impl From<&str> for SpanKeyParseError {
    fn from(msg: &str) -> Self {
        SpanKeyParseError {
            details: msg.to_string(),
        }
    }
}

impl From<String> for SpanKeyParseError {
    fn from(msg: String) -> Self {
        SpanKeyParseError { details: msg }
    }
}