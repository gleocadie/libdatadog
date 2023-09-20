// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0. This product includes software
// developed at Datadog (https://www.datadoghq.com/). Copyright 2023-Present
// Datadog, Inc.

use datadog_trace_protobuf::pb;
use log::debug;

use crate::{
    http::obfuscate_url_string, json::obfuscate_json_string, memcached::obfuscate_memcached_string,
    obfuscation_config::ObfuscationConfig, replacer::replace_span_tags, sql::obfuscate_sql_string,
};

const TEXT_NON_PARSABLE: &str = "Non-parsable SQL query";

pub fn obfuscate_span(span: &mut pb::Span, config: &ObfuscationConfig) {
    match span.r#type.as_str() {
        "web" | "http" => {
            if span.meta.is_empty() {
                return;
            }
            if let Some(url) = span.meta.get_mut("http.url") {
                *url = obfuscate_url_string(
                    url,
                    config.http_remove_query_string,
                    config.http_remove_path_digits,
                )
            }
        }
        "memcached" if config.obfuscate_memcached => {
            if let Some(cmd) = span.meta.get_mut("memcached.command") {
                *cmd = obfuscate_memcached_string(cmd)
            }
        }
        "sql" | "cassandra" => {
            if span.resource.is_empty() || !config.obfuscate_sql {
                return;
            }
            let sql_obfuscation_result = obfuscate_sql_string(&span.resource, config);
            if let Some(err) = sql_obfuscation_result.error {
                debug!(
                    "Error parsing SQL query: {}. Resource: {}",
                    err, span.resource
                );
                span.resource = TEXT_NON_PARSABLE.to_string();
                span.meta
                    .insert("sql.query".to_string(), TEXT_NON_PARSABLE.to_string());
            }
            let query = sql_obfuscation_result.obfuscated_string.unwrap_or_default();
            span.resource = query.clone();
            span.meta.insert("sql.query".to_string(), query);
        }
        "mongodb" => {
            if !span.meta.contains_key("mongodb.query") || !config.obfuscate_mongodb {
                return;
            }
            let mongodb_string = &span.meta["mongodb.query"];
            span.meta.insert(
                "mongodb.query".to_string(),
                obfuscate_json_string(
                    config,
                    crate::json::JSONObfuscationType::MongoDB,
                    mongodb_string,
                ),
            );
        }
        "elasticsearch" => {
            if !span.meta.contains_key("elasticsearch.body") || !config.obfuscate_elasticsearch {
                return;
            }
            let elasticsearch_string = &span.meta["elasticsearch.body"];
            span.meta.insert(
                "elasticsearch.body".to_string(),
                obfuscate_json_string(
                    config,
                    crate::json::JSONObfuscationType::Elasticsearch,
                    elasticsearch_string,
                ),
            );
        }
        _ => {}
    }
    if let Some(tag_replace_rules) = &config.tag_replace_rules {
        replace_span_tags(span, tag_replace_rules)
    }
}

#[cfg(test)]
mod tests {
    use datadog_trace_utils::trace_test_utils;
    use serde_json::json;

    use crate::{obfuscation_config::ObfuscationConfig, replacer};

    use super::obfuscate_span;

    #[test]
    fn test_obfuscates_span_url_strings() {
        let mut span = trace_test_utils::create_test_span(111, 222, 0, 1, true);
        span.r#type = "http".to_string();
        span.meta.insert(
            "http.url".to_string(),
            "http://foo.com/id/123/page/q?search=bar&page=2".to_string(),
        );
        let mut obf_config = ObfuscationConfig::new_test_config();
        obf_config.http_remove_query_string = true;
        obf_config.http_remove_path_digits = true;
        obfuscate_span(&mut span, &obf_config);
        assert_eq!(
            span.meta.get("http.url").unwrap(),
            "http://foo.com/id/?/page/q?"
        )
    }

    #[test]
    fn test_replace_span_tags() {
        let mut span = trace_test_utils::create_test_span(111, 222, 0, 1, true);
        span.meta
            .insert("custom.tag".to_string(), "/foo/bar/foo".to_string());

        let parsed_rules = replacer::parse_rules_from_string(
            r#"[{"name": "custom.tag", "pattern": "(/foo/bar/).*", "repl": "${1}extra"}]"#,
        )
        .unwrap();
        let mut obf_config = ObfuscationConfig::new_test_config();
        obf_config.tag_replace_rules = Some(parsed_rules);

        obfuscate_span(&mut span, &obf_config);

        assert_eq!(span.meta.get("custom.tag").unwrap(), "/foo/bar/extra");
    }

    #[test]
    fn test_obfuscate_span_sql_query() {
        let mut span = trace_test_utils::create_test_span(111, 222, 0, 1, true);
        span.resource = "SELECT username AS name from users where id = 2".to_string();
        span.r#type = "sql".to_string();
        let mut obf_config = ObfuscationConfig::new_test_config();
        obf_config.obfuscate_sql = true;

        obfuscate_span(&mut span, &obf_config);
        assert_eq!(span.resource, "SELECT username from users where id = ?");
        assert_eq!(
            span.meta.get("sql.query").unwrap(),
            "SELECT username from users where id = ?"
        )
    }

    #[test]
    fn test_obfuscate_mongodb_query() {
        let mut span = trace_test_utils::create_test_span(111, 222, 0, 1, true);
        span.r#type = "mongodb".to_string();
        span.meta.insert(
            "mongodb.query".to_string(),
            json!( { "key": "val", "obj": { "arr": ["a", "b"]}}).to_string(),
        );
        let mut obf_config = ObfuscationConfig::new_test_config();
        obf_config.obfuscate_mongodb = true;

        obfuscate_span(&mut span, &obf_config);
        assert_eq!(
            span.meta.get("mongodb.query").unwrap().to_string(),
            json!( { "key": "?", "obj": { "arr": ["?", "?"]}} ).to_string()
        )
    }

    #[test]
    fn test_obfuscate_elasticsearch_query() {
        let mut span = trace_test_utils::create_test_span(111, 222, 0, 1, true);
        span.r#type = "elasticsearch".to_string();
        span.meta.insert(
            "elasticsearch.body".to_string(),
            json!( { "key": "val", "obj": { "arr": ["a", "b"]}}).to_string(),
        );
        let mut obf_config = ObfuscationConfig::new_test_config();
        obf_config.obfuscate_elasticsearch = true;

        obfuscate_span(&mut span, &obf_config);
        assert_eq!(
            span.meta.get("elasticsearch.body").unwrap().to_string(),
            json!( { "key": "?", "obj": { "arr": ["?", "?"]}} ).to_string()
        )
    }
}
