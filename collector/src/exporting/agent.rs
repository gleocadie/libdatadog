// #[macro_use]
// extern crate serde_derive;

extern crate serde;
extern crate serde_json;

use crate::commands::{Command, UpdateSamplingRatesCommand};
use crate::config::Config;
use crate::metadata::ProcessInfo;
use crate::runtime::RUNTIME;
use crate::tracing::{Segment, Segments, Span, Meta, Metrics};
use hyper::{body, Body, Method, Request};
use hyper::client::Client;
use rmp::encode;
use rmp::encode::ByteBuf;
use serde::{Serialize, Deserialize};
use tokio::sync::broadcast::Sender;
use std::collections::HashMap;
use std::sync::Arc as Rc;

pub struct AgentExporter {
    // client: Box<dyn Client + Send + Sync>,
    host: String,
    tx: Sender<Command>
}

#[derive(Serialize, Deserialize)]
struct AgentResponse {
    rate_by_service: HashMap<String, f32>,
}

impl AgentExporter {
    pub fn new(tx: Sender<Command>) -> Self {
        Self {
            host: String::from("http://127.0.0.1"),
            tx
        }
    }

    pub fn configure(&mut self, config: Config) {
        self.host = config.host.clone();
    }

    pub fn export(&self, traces: Segments, process_info: &ProcessInfo) {
        let mut wr = ByteBuf::new();
        let trace_count = traces.len();

        // println!("{:#?}", trace_count);

        if trace_count > 0 {
            // println!("{:#?}", traces);

            self.encode_segments(&mut wr, traces);

            let url = format!("{}{}", self.host, "/v0.5/traces");
            let data: Vec<u8> = wr.as_vec().to_vec();
            let req = Request::builder()
                .method(Method::PUT)
                .uri(url)
                .header("Content-Type", "application/msgpack")
                .header("Datadog-Meta-Lang", process_info.language.clone())
                .header("Datadog-Meta-Version", process_info.language_interpreter.clone())
                .header("Datadog-Meta-Interpreter", process_info.language_version.clone())
                .header("Datadog-Meta-Tracer-Version", process_info.tracer_version.clone())
                .header("X-Datadog-Trace-Count", trace_count.to_string())
                .body(Body::from(data))
                .unwrap();

            let tx = self.tx.clone();

            RUNTIME.spawn(async move {
                let client = Client::new(); // TODO: reuse client by session
                let res = client.request(req).await.unwrap();
                let body = body::to_bytes(res.into_body()).await.unwrap();
                let str = String::from_utf8(body.to_vec()).unwrap();
                let json: AgentResponse = serde_json::from_str(str.as_str()).unwrap();
                let rate_by_service = json.rate_by_service;

                tx.send(Command::UpdateSamplingRates(UpdateSamplingRatesCommand {
                    rate_by_service
                })).unwrap();
            });
        }
    }

    fn cache_strings(&self, strings: &mut Vec<Rc<str>>, positions: &mut HashMap<Rc<str>, u32>, trace: &Segment) {
        for span in trace.spans.values() {
            self.cache_string(strings, positions, &span.service);
            self.cache_string(strings, positions, &span.name);
            self.cache_string(strings, positions, &span.resource);
            self.cache_string(strings, positions, &span.span_type);

            for (k, v) in &span.meta {
                self.cache_string(strings, positions, &k);
                self.cache_string(strings, positions, &v);
            }

            for (k, _) in &span.metrics {
                self.cache_string(strings, positions, &k);
            }
        }
    }

    fn cache_string(&self, strings: &mut Vec<Rc<str>>, positions: &mut HashMap<Rc<str>, u32>, s: &Rc<str>) {
        if !positions.contains_key(s) {
            let len = strings.len() as u32;

            positions.insert(s.clone(), len);
            strings.push(s.clone());
        }
    }

    fn encode_strings(&self, wr: &mut ByteBuf, strings: &mut Vec<Rc<str>>) {
        encode::write_array_len(wr, strings.len() as u32).unwrap();

        for s in strings {
            encode::write_str(wr, s).unwrap();
        }
    }

    fn encode_segments(&self, wr: &mut ByteBuf, segments: Segments) {
        encode::write_array_len(wr, 2).unwrap();

        let empty_string: Rc<str> = Rc::from("");
        let mut strings = Vec::new();
        let mut positions = HashMap::new();

        strings.push(empty_string.clone());
        positions.insert(empty_string.clone(), 0u32);

        // TODO: Avoid looping twice over segments/strings.
        for segment in segments.values() {
            self.cache_strings(&mut strings, &mut positions, segment);
        }

        self.encode_strings(wr, &mut strings);

        encode::write_array_len(wr, segments.len() as u32).unwrap();

        for segment in segments.values() {
            self.encode_segment(wr, segment, &positions);
        }
    }

    fn encode_segment(&self, wr: &mut ByteBuf, segment: &Segment, positions: &HashMap<Rc<str>, u32>) {
        encode::write_array_len(wr, segment.spans.len() as u32).unwrap();

        for span in segment.spans.values() {
            self.encode_span(wr, segment, span, positions);
        }
    }

    fn encode_span(&self, wr: &mut ByteBuf, segment: &Segment, span: &Span, positions: &HashMap<Rc<str>, u32>) {
        let trace_id = u64::try_from(segment.trace_id >> 64).unwrap(); // TODO: lower bits
        encode::write_array_len(wr, 12).unwrap();

        encode::write_uint(wr, positions[&span.service] as u64).unwrap();
        encode::write_uint(wr, positions[&span.name] as u64).unwrap();
        encode::write_uint(wr, positions[&span.resource] as u64).unwrap();
        encode::write_uint(wr, trace_id).unwrap();
        encode::write_uint(wr, span.span_id).unwrap();
        encode::write_uint(wr, span.parent_id).unwrap();
        encode::write_uint(wr, span.start).unwrap();
        encode::write_uint(wr, span.duration + 1).unwrap();
        encode::write_uint(wr, span.error).unwrap();
        self.encode_meta(wr, &span.meta, positions);
        self.encode_metrics(wr, &span.metrics, positions);
        encode::write_uint(wr, positions[&span.span_type] as u64).unwrap();
    }

    fn encode_meta(&self, wr: &mut ByteBuf, meta: &Meta, positions: &HashMap<Rc<str>, u32>) {
        encode::write_map_len(wr, meta.len() as u32).unwrap();

        for (k, v) in meta {
            encode::write_uint(wr, positions[k] as u64).unwrap();
            encode::write_uint(wr, positions[v] as u64).unwrap();
        }
    }

    fn encode_metrics(&self, wr: &mut ByteBuf, metrics: &Metrics, positions: &HashMap<Rc<str>, u32>) {
        encode::write_map_len(wr, metrics.len() as u32).unwrap();

        for (k, v) in metrics {
            encode::write_uint(wr, positions[k] as u64).unwrap();
            encode::write_f64(wr, *v).unwrap();
        }
    }
}
