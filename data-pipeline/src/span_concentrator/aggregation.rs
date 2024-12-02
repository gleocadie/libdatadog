// Copyright 2024-Present Datadog, Inc. https://www.datadoghq.com/
// SPDX-License-Identifier: Apache-2.0
//! This module implement the logic for stats aggregation into time buckets and stats group.
//! This includes the aggregation key to group spans together and the computation of stats from a
//! span.
use datadog_trace_protobuf::pb;
use datadog_trace_utils::span_v04::{trace_utils, Span};
use std::borrow::Borrow;
use std::borrow::Cow;
use std::collections::HashMap;
use tinybytes::BytesString;

const TAG_STATUS_CODE: &str = "http.status_code";
const TAG_SYNTHETICS: &str = "synthetics";
const TAG_SPANKIND: &str = "span.kind";
const TAG_ORIGIN: &str = "_dd.origin";

/// This struct represent the key used to group spans together to compute stats.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Default)]
pub(super) struct AggregationKey<'a, T>
where
    T: Borrow<str>,
{
    resource_name: T,
    service_name: T,
    operation_name: T,
    span_type: T,
    span_kind: T,
    http_status_code: u32,
    is_synthetics_request: bool,
    peer_tags: Vec<(Cow<'a, str>, T)>,
    is_trace_root: bool,
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub(super) struct BorrowedAggregationKey<'a> {
    resource_name: &'a str,
    service_name: &'a str,
    operation_name: &'a str,
    span_type: &'a str,
    span_kind: &'a str,
    http_status_code: u32,
    is_synthetics_request: bool,
    peer_tags: Vec<(&'a str, &'a str)>,
    is_trace_root: bool,
}

trait BorrowedAggregationKeyHelper {
    fn borrowed_aggregation_key(&self) -> BorrowedAggregationKey;
}

impl<T> BorrowedAggregationKeyHelper for AggregationKey<'_, T>
where
    T: Borrow<str>,
{
    fn borrowed_aggregation_key(&self) -> BorrowedAggregationKey {
        BorrowedAggregationKey {
            resource_name: self.resource_name.borrow(),
            service_name: self.service_name.borrow(),
            operation_name: self.operation_name.borrow(),
            span_type: self.span_type.borrow(),
            span_kind: self.span_kind.borrow(),
            http_status_code: self.http_status_code,
            is_synthetics_request: self.is_synthetics_request,
            peer_tags: self
                .peer_tags
                .iter()
                .map(|(tag, value)| (tag.as_ref(), value.borrow()))
                .collect(),
            is_trace_root: self.is_trace_root,
        }
    }
}

impl BorrowedAggregationKeyHelper for BorrowedAggregationKey<'_> {
    fn borrowed_aggregation_key(&self) -> BorrowedAggregationKey {
        self.clone()
    }
}

impl<'a, 'b, T> Borrow<dyn BorrowedAggregationKeyHelper + 'b> for AggregationKey<'a, T>
where
    T: Borrow<str> + 'a,
    'a: 'b,
{
    fn borrow(&self) -> &(dyn BorrowedAggregationKeyHelper + 'b) {
        self
    }
}

impl Eq for (dyn BorrowedAggregationKeyHelper + '_) {}

impl PartialEq for (dyn BorrowedAggregationKeyHelper + '_) {
    fn eq(&self, other: &dyn BorrowedAggregationKeyHelper) -> bool {
        self.borrowed_aggregation_key()
            .eq(&other.borrowed_aggregation_key())
    }
}

impl<'a> std::hash::Hash for (dyn BorrowedAggregationKeyHelper + 'a) {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.borrowed_aggregation_key().hash(state)
    }
}

impl<'a> AggregationKey<'a, BytesString> {
    /// Return an AggregationKey matching the given span.
    ///
    /// If `peer_tags_keys` is not empty then the peer tags of the span will be included in the
    /// key.
    pub(super) fn from_span(span: &Span, peer_tag_keys: &'a [&'a str]) -> Self {
        let span_kind = span
            .meta
            .get(TAG_SPANKIND)
            .map(|s| s)
            .cloned()
            .unwrap_or_default();
        let peer_tags = if client_or_producer(span_kind.as_str()) {
            get_peer_tags(span, peer_tag_keys)
        } else {
            vec![]
        };
        Self {
            resource_name: span.resource.clone(),
            service_name: span.service.clone(),
            operation_name: span.name.clone(),
            span_type: span.r#type.clone(),
            span_kind,
            http_status_code: get_status_code(span),
            is_synthetics_request: span
                .meta
                .get(TAG_ORIGIN)
                .is_some_and(|origin| origin.as_str().starts_with(TAG_SYNTHETICS)),
            is_trace_root: span.parent_id == 0,
            peer_tags: peer_tags
                .into_iter()
                .map(|(k, v)| (Cow::from(k), v))
                .collect(),
        }
    }

    pub(super) fn to_string_key(self) -> AggregationKey<'static, String> {
        AggregationKey::<String> {
            resource_name: self.resource_name.copy_to_string(),
            service_name: self.service_name.copy_to_string(),
            operation_name: self.operation_name.copy_to_string(),
            span_type: self.span_type.copy_to_string(),
            span_kind: self.span_kind.copy_to_string(),
            http_status_code: self.http_status_code,
            is_synthetics_request: self.is_synthetics_request,
            is_trace_root: self.is_trace_root,
            peer_tags: self
                .peer_tags
                .into_iter()
                .map(|(key, value)| (Cow::from(key.into_owned()), value.copy_to_string()))
                .collect(),
        }
    }
}

impl From<pb::ClientGroupedStats> for AggregationKey<'static, String> {
    fn from(value: pb::ClientGroupedStats) -> Self {
        Self {
            resource_name: value.resource,
            service_name: value.service,
            operation_name: value.name,
            span_type: value.r#type,
            span_kind: value.span_kind,
            http_status_code: value.http_status_code,
            is_synthetics_request: value.synthetics,
            peer_tags: value
                .peer_tags
                .into_iter()
                .filter_map(|t| {
                    let (key, value) = t.split_once(":")?;
                    Some((Cow::from(key.to_string()), value.to_string()))
                })
                .collect(),
            is_trace_root: value.is_trace_root == 1,
        }
    }
}

/// Return the status code of a span based on the metrics and meta tags.
fn get_status_code(span: &Span) -> u32 {
    if let Some(status_code) = span.metrics.get(TAG_STATUS_CODE) {
        *status_code as u32
    } else if let Some(status_code) = span.meta.get(TAG_STATUS_CODE) {
        status_code.as_str().parse().unwrap_or(0)
    } else {
        0
    }
}

/// Return true if the span kind is "client" or "producer"
fn client_or_producer(span_kind: &str) -> bool {
    matches!(span_kind.to_lowercase().as_str(), "client" | "producer")
}

/// Parse the meta tags of a span and return a list of the peer tags based on the list of
/// `peer_tag_keys`
fn get_peer_tags<'a>(span: &'_ Span, peer_tag_keys: &'a [&'a str]) -> Vec<(&'a str, BytesString)> {
    peer_tag_keys
        .iter()
        .filter_map(|key| Some((*key, span.meta.get(*key).cloned()?)))
        .collect()
}

/// The stats computed from a group of span with the same AggregationKey
#[derive(Debug, Default, Clone)]
pub(super) struct GroupedStats {
    hits: u64,
    errors: u64,
    duration: u64,
    top_level_hits: u64,
    ok_summary: datadog_ddsketch::DDSketch,
    error_summary: datadog_ddsketch::DDSketch,
}

impl GroupedStats {
    /// Update the stats of a GroupedStats by inserting a span.
    fn insert(&mut self, value: &Span) {
        self.hits += 1;
        self.duration += value.duration as u64;

        if value.error != 0 {
            self.errors += 1;
            let _ = self.error_summary.add(value.duration as f64);
        } else {
            let _ = self.ok_summary.add(value.duration as f64);
        }
        if trace_utils::has_top_level(value) {
            self.top_level_hits += 1;
        }
    }
}

/// A time bucket used for stats aggregation. It stores a map of GroupedStats storing the stats of
/// spans aggregated on their AggregationKey.
#[derive(Debug, Clone)]
pub(super) struct StatsBucket {
    data: HashMap<AggregationKey<'static, String>, GroupedStats>,
    start: u64,
}

impl StatsBucket {
    /// Return a new StatsBucket starting at the given timestamp
    pub(super) fn new(start_timestamp: u64) -> Self {
        Self {
            data: HashMap::new(),
            start: start_timestamp,
        }
    }

    /// Insert a value as stats in the group corresponding to the aggregation key, if it does
    /// not exist it creates it.
    pub(super) fn insert(&mut self, key: AggregationKey<'_, BytesString>, value: &Span) {
        if let Some(grouped_stats) = self.data.get_mut(&key as &dyn BorrowedAggregationKeyHelper) {
            grouped_stats.insert(value);
        } else {
            let mut grouped_stats = GroupedStats::default();
            grouped_stats.insert(value);
            self.data.insert(key.to_string_key(), grouped_stats);
        }
    }

    /// Consume the bucket and return a ClientStatsBucket containing the bucket stats.
    /// `bucket_duration` is the size of buckets for the concentrator containing the bucket.
    pub(super) fn flush(self, bucket_duration: u64) -> pb::ClientStatsBucket {
        pb::ClientStatsBucket {
            start: self.start,
            duration: bucket_duration,
            stats: self
                .data
                .into_iter()
                .map(|(k, b)| encode_grouped_stats(k, b))
                .collect(),
            // Agent-only field
            agent_time_shift: 0,
        }
    }
}

/// Create a ClientGroupedStats struct based on the given AggregationKey and GroupedStats
fn encode_grouped_stats(
    key: AggregationKey<String>,
    group: GroupedStats,
) -> pb::ClientGroupedStats {
    pb::ClientGroupedStats {
        service: key.service_name,
        name: key.operation_name,
        resource: key.resource_name,
        http_status_code: key.http_status_code,
        r#type: key.span_type,
        db_type: String::new(), // db_type is not used yet (see proto definition)

        hits: group.hits,
        errors: group.errors,
        duration: group.duration,

        ok_summary: group.ok_summary.encode_to_vec(),
        error_summary: group.error_summary.encode_to_vec(),
        synthetics: key.is_synthetics_request,
        top_level_hits: group.top_level_hits,
        span_kind: key.span_kind,

        peer_tags: key
            .peer_tags
            .into_iter()
            .map(|(k, v)| format!("{k}:{v}"))
            .collect(),
        is_trace_root: if key.is_trace_root {
            pb::Trilean::True.into()
        } else {
            pb::Trilean::False.into()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregation_key_from_span() {
        let test_cases: Vec<(Span, AggregationKey<BytesString>)> = vec![
            // Root span
            (
                Span {
                    service: "service".into(),
                    name: "op".into(),
                    resource: "res".into(),
                    span_id: 1,
                    parent_id: 0,
                    ..Default::default()
                },
                AggregationKey {
                    service_name: "service".into(),
                    operation_name: "op".into(),
                    resource_name: "res".into(),
                    is_trace_root: true,
                    ..Default::default()
                },
            ),
            // Span with span kind
            (
                Span {
                    service: "service".into(),
                    name: "op".into(),
                    resource: "res".into(),
                    span_id: 1,
                    parent_id: 0,
                    meta: HashMap::from([("span.kind".into(), "client".into())]),
                    ..Default::default()
                },
                AggregationKey {
                    service_name: "service".into(),
                    operation_name: "op".into(),
                    resource_name: "res".into(),
                    span_kind: "client".into(),
                    is_trace_root: true,
                    ..Default::default()
                },
            ),
            // Span with peer tags but peertags aggregation disabled
            (
                Span {
                    service: "service".into(),
                    name: "op".into(),
                    resource: "res".into(),
                    span_id: 1,
                    parent_id: 0,
                    meta: HashMap::from([
                        ("span.kind".into(), "client".into()),
                        ("aws.s3.bucket".into(), "bucket-a".into()),
                    ]),
                    ..Default::default()
                },
                AggregationKey {
                    service_name: "service".into(),
                    operation_name: "op".into(),
                    resource_name: "res".into(),
                    span_kind: "client".into(),
                    is_trace_root: true,
                    ..Default::default()
                },
            ),
            // Span with multiple peer tags but peertags aggregation disabled
            (
                Span {
                    service: "service".into(),
                    name: "op".into(),
                    resource: "res".into(),
                    span_id: 1,
                    parent_id: 0,
                    meta: HashMap::from([
                        ("span.kind".into(), "producer".into()),
                        ("aws.s3.bucket".into(), "bucket-a".into()),
                        ("db.instance".into(), "dynamo.test.us1".into()),
                        ("db.system".into(), "dynamodb".into()),
                    ]),
                    ..Default::default()
                },
                AggregationKey {
                    service_name: "service".into(),
                    operation_name: "op".into(),
                    resource_name: "res".into(),
                    span_kind: "producer".into(),
                    is_trace_root: true,
                    ..Default::default()
                },
            ),
            // Span with multiple peer tags but peertags aggregation disabled and span kind is
            // server
            (
                Span {
                    service: "service".into(),
                    name: "op".into(),
                    resource: "res".into(),
                    span_id: 1,
                    parent_id: 0,
                    meta: HashMap::from([
                        ("span.kind".into(), "server".into()),
                        ("aws.s3.bucket".into(), "bucket-a".into()),
                        ("db.instance".into(), "dynamo.test.us1".into()),
                        ("db.system".into(), "dynamodb".into()),
                    ]),
                    ..Default::default()
                },
                AggregationKey {
                    service_name: "service".into(),
                    operation_name: "op".into(),
                    resource_name: "res".into(),
                    span_kind: "server".into(),
                    is_trace_root: true,
                    ..Default::default()
                },
            ),
            // Span from synthetics
            (
                Span {
                    service: "service".into(),
                    name: "op".into(),
                    resource: "res".into(),
                    span_id: 1,
                    parent_id: 0,
                    meta: HashMap::from([("_dd.origin".into(), "synthetics-browser".into())]),
                    ..Default::default()
                },
                AggregationKey {
                    service_name: "service".into(),
                    operation_name: "op".into(),
                    resource_name: "res".into(),
                    is_synthetics_request: true,
                    is_trace_root: true,
                    ..Default::default()
                },
            ),
            // Span with status code in meta
            (
                Span {
                    service: "service".into(),
                    name: "op".into(),
                    resource: "res".into(),
                    span_id: 1,
                    parent_id: 0,
                    meta: HashMap::from([("http.status_code".into(), "418".into())]),
                    ..Default::default()
                },
                AggregationKey {
                    service_name: "service".into(),
                    operation_name: "op".into(),
                    resource_name: "res".into(),
                    is_synthetics_request: false,
                    is_trace_root: true,
                    http_status_code: 418,
                    ..Default::default()
                },
            ),
            // Span with invalid status code in meta
            (
                Span {
                    service: "service".into(),
                    name: "op".into(),
                    resource: "res".into(),
                    span_id: 1,
                    parent_id: 0,
                    meta: HashMap::from([("http.status_code".into(), "x".into())]),
                    ..Default::default()
                },
                AggregationKey {
                    service_name: "service".into(),
                    operation_name: "op".into(),
                    resource_name: "res".into(),
                    is_synthetics_request: false,
                    is_trace_root: true,
                    ..Default::default()
                },
            ),
            // Span with status code in metrics
            (
                Span {
                    service: "service".into(),
                    name: "op".into(),
                    resource: "res".into(),
                    span_id: 1,
                    parent_id: 0,
                    metrics: HashMap::from([("http.status_code".into(), 418.0)]),
                    ..Default::default()
                },
                AggregationKey {
                    service_name: "service".into(),
                    operation_name: "op".into(),
                    resource_name: "res".into(),
                    is_synthetics_request: false,
                    is_trace_root: true,
                    http_status_code: 418,
                    ..Default::default()
                },
            ),
        ];

        let test_peer_tags = vec!["aws.s3.bucket", "db.instance", "db.system"];

        let test_cases_with_peer_tags: Vec<(Span, AggregationKey<BytesString>)> = vec![
            // Span with peer tags with peertags aggregation enabled
            (
                Span {
                    service: "service".into(),
                    name: "op".into(),
                    resource: "res".into(),
                    span_id: 1,
                    parent_id: 0,
                    meta: HashMap::from([
                        ("span.kind".into(), "client".into()),
                        ("aws.s3.bucket".into(), "bucket-a".into()),
                    ]),
                    ..Default::default()
                },
                AggregationKey {
                    service_name: "service".into(),
                    operation_name: "op".into(),
                    resource_name: "res".into(),
                    span_kind: "client".into(),
                    is_trace_root: true,
                    peer_tags: vec![("aws.s3.bucket".into(), "bucket-a".into())],
                    ..Default::default()
                },
            ),
            // Span with multiple peer tags with peertags aggregation enabled
            (
                Span {
                    service: "service".into(),
                    name: "op".into(),
                    resource: "res".into(),
                    span_id: 1,
                    parent_id: 0,
                    meta: HashMap::from([
                        ("span.kind".into(), "producer".into()),
                        ("aws.s3.bucket".into(), "bucket-a".into()),
                        ("db.instance".into(), "dynamo.test.us1".into()),
                        ("db.system".into(), "dynamodb".into()),
                    ]),
                    ..Default::default()
                },
                AggregationKey {
                    service_name: "service".into(),
                    operation_name: "op".into(),
                    resource_name: "res".into(),
                    span_kind: "producer".into(),
                    peer_tags: vec![
                        ("aws.s3.bucket".into(), "bucket-a".into()),
                        ("db.instance".into(), "dynamo.test.us1".into()),
                        ("db.system".into(), "dynamodb".into()),
                    ],
                    is_trace_root: true,
                    ..Default::default()
                },
            ),
            // Span with multiple peer tags with peertags aggregation enabled and span kind is
            // server
            (
                Span {
                    service: "service".into(),
                    name: "op".into(),
                    resource: "res".into(),
                    span_id: 1,
                    parent_id: 0,
                    meta: HashMap::from([
                        ("span.kind".into(), "server".into()),
                        ("aws.s3.bucket".into(), "bucket-a".into()),
                        ("db.instance".into(), "dynamo.test.us1".into()),
                        ("db.system".into(), "dynamodb".into()),
                    ]),
                    ..Default::default()
                },
                AggregationKey {
                    service_name: "service".into(),
                    operation_name: "op".into(),
                    resource_name: "res".into(),
                    span_kind: "server".into(),
                    is_trace_root: true,
                    ..Default::default()
                },
            ),
        ];

        for (span, expected_key) in test_cases {
            assert_eq!(AggregationKey::from_span(&span, &[]), expected_key);
        }

        for (span, expected_key) in test_cases_with_peer_tags {
            assert_eq!(
                AggregationKey::from_span(&span, &test_peer_tags),
                expected_key
            );
        }
    }
}
