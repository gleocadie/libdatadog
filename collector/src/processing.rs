use crate::commands::Command;
use crate::config::Config;
use crate::events::{AddTagsEvent, ErrorEvent, Event, ExceptionEvent, FinishSegmentEvent, FinishSpanEvent, SamplingPriorityEvent, StartSegmentEvent, StartSpanEvent};
use crate::exporting::agent::AgentExporter;
use crate::metadata::ProcessInfo;
use crate::tracing::{Span, Segment, Segments};
use tokio::sync::broadcast::{self, Sender, Receiver};
use std::collections::{HashMap, HashSet};
// TODO: Figure out how to use the faster std::rc::Rc
// TODO: Consider using imstr instead to slice directly from the string table.
use std::sync::Arc as Rc;

pub struct Processor {
    exporter: AgentExporter,
    segments: Segments,
    strings: HashSet<Rc<str>>,
    process_info: Option<ProcessInfo>,
    tx: Sender<Command>,
}

// TODO: Use msgpack extension for string table and switch everything to serde.
// TODO: Decouple processing from exporting.
// TODO: Add support for more payload metadata (i.e. language).
// TODO: Custom more efficient events depending on span type.
// TODO: Store service metadata that can be used on every span like service name.
// TODO: Cache things like outgoing host/port or MySQL connection information.
// TODO: Event for adding trace tags.
// TODO: Event for adding baggage items.
// TODO: Add support for sampling.
// TODO: Support sending traces directly to Datadog.
// TODO: Optimize to minimize allocations and copies.

impl Processor {
    // Box<dyn Exporter + Send + Sync>
    pub fn new() -> Self {
        let (tx, _): (Sender<Command>, _) = broadcast::channel(1000);

        Self {
            exporter: AgentExporter::new(tx.clone()),
            segments: Segments::new(),
            // TODO: Figure out how to cache those properly.
            strings: HashSet::from([Rc::from("")]),
            process_info: None,
            tx,
        }
    }

    fn add_tags(span: &mut Span, meta: HashMap<Rc<str>, Rc<str>>, metrics: HashMap<Rc<str>, f64>) {
        for (k, v) in meta {
            match &*k {
                "resource.name" => span.resource = v,
                "span.name" => span.name = v,
                "span.type" => span.span_type = v,
                "service.name" => span.service = v,
                "http.route" => {
                    if &*span.span_type == "web" && span.resource.is_empty() {
                        let resource = [span.meta["http.method"].clone(), v];
                        span.resource = Rc::from(resource.join(" ").trim());
                    }
                },
                "error.message" | "error.type" | "error.stack" => {
                    span.error = 1;
                    span.meta.insert(k, v);
                },
                _ => { span.meta.insert(k, v); }
            }
        }

        for (k, v) in metrics {
            match &*k {
                "http.status_code" => { span.meta.insert(k, Rc::from(v.to_string())); },
                _ => { span.metrics.insert(k, v); }
            }
        }
    }

    pub fn flush(&mut self) {
        let finished_traces: HashMap<u64, Segment> = self.segments
            .extract_if(|_, v| v.started == v.finished)
            .collect();

        if finished_traces.len() == 0 { return }

        match &self.process_info {
            Some(info) => {
                self.exporter.export(finished_traces, info);
            },
            None => {
                println!("Process information is required to submit traces.");
            }
        }
    }

    pub fn process(&mut self, event: Event) {
        match event {
            Event::StartSegment(event) => self.process_start_segment(event),
            Event::FinishSegment(event) => self.process_finish_segment(event),
            Event::StartSpan(event) => self.process_start_span(event),
            Event::FinishSpan(event) => self.process_finish_span(event),
            Event::AddTags(event) => self.process_add_tags(event),
            Event::Exception(event) => self.process_exception(event),
            Event::Error(event) => self.process_error(event),
            Event::Config(config) => self.process_config(config),
            Event::ProcessInfo(info) => self.process_process_info(info),
            Event::SamplingPriority(event) => self.process_sampling_priority(event),
            Event::FlushTraces => self.flush(),
        }
    }

    pub fn subscribe(&mut self) -> Receiver<Command> {
        self.tx.subscribe()
    }

    // TODO: Store an error object instead of tags on the span.
    fn process_exception(&mut self, event: ExceptionEvent) {
        let message_key = self.from_str("error.message");
        let name_key = self.from_str("error.type");
        let stack_key = self.from_str("error.stack");

        if let Some(segment) = self.segments.get_mut(&event.segment_id) {
            if let Some(span) = segment.spans.get_mut(&event.span_id) {
                span.error = 1;

                span.meta.insert(message_key, event.message);
                span.meta.insert(name_key, event.name);
                span.meta.insert(stack_key, event.stack);
            }
        }
    }

    fn process_error(&mut self, event: ErrorEvent) {
        if let Some(segment) = self.segments.get_mut(&event.segment_id) {
            if let Some(span) = segment.spans.get_mut(&event.span_id) {
                span.error = 1;
            }
        }
    }

    fn process_add_tags(&mut self, event: AddTagsEvent) {
        if let Some(trace) = self.segments.get_mut(&event.segment_id) {
            if let Some(mut span) = trace.spans.get_mut(&event.span_id) {
                Self::add_tags(&mut span, event.meta, event.metrics);
            }
        }
    }

    fn process_start_segment(&mut self, event: StartSegmentEvent) {
        let segment = Segment {
            start: event.time,
            trace_id: event.trace_id,
            started: 0,
            finished: 0,
            root: 0,
            spans: HashMap::new(),
        };

        self.segments.insert(event.segment_id, segment);
    }

    fn process_finish_segment(&mut self, event: FinishSegmentEvent) {
        let segment = self.segments.get_mut(&event.segment_id);

        if let Some(segment) = segment {
            for (_, span) in &mut segment.spans {
                if span.duration == 0 {
                    span.duration = segment.start + event.ticks - span.start;
                }
            }

            segment.finished = segment.started
        }
    }

    fn process_start_span(&mut self, event: StartSpanEvent) {
        let segment = self.segments.get_mut(&event.segment_id).unwrap();
        let start = segment.start + event.ticks;
        let mut span = Span {
            start,
            span_id: event.span_id,
            parent_id: event.parent_id,
            span_type: event.span_type,
            name: event.name,
            resource: event.resource,
            service: event.service,
            error: 0,
            duration: 0,
            meta: HashMap::new(),
            metrics: HashMap::new()
        };

        Self::add_tags(&mut span, event.meta, event.metrics);

        if segment.root == 0 {
            segment.root = span.span_id;
        }

        segment.started += 1;
        segment.spans.insert(span.span_id, span);
    }

    fn process_finish_span(&mut self, event: FinishSpanEvent) {
        if let Some(segment) = self.segments.get_mut(&event.segment_id) {
            if let Some(span) = segment.spans.get_mut(&event.span_id) {
                segment.finished += 1;
                span.duration = segment.start + event.ticks - span.start;
            }
        }
    }

    fn process_sampling_priority(&mut self, event: SamplingPriorityEvent) {
        let priority_key = self.from_str("error.stack");

        if let Some(segment) = self.segments.get_mut(&event.segment_id) {
            if let Some(span) = segment.spans.get_mut(&segment.root) {
                span.metrics.insert(priority_key, event.priority as f64);
            }
        }
    }

    fn process_config(&mut self, config: Config) {
        self.exporter.configure(config);
    }

    fn process_process_info(&mut self, info: ProcessInfo) {
        self.process_info = Some(info);
    }

    // fn process_flush_traces(&mut self, _: &[Rc<str>]) {
    //     // println!("{}", "flushing");
    //     // self.flush();
    // }

    // fn read_tags<R: Read>(&self, mut rd: R, strings: &[Rc<str>]) -> (Meta, Metrics){
    //     let mut meta = HashMap::new();
    //     let mut metrics = HashMap::new();

    //     let meta_size = read_map_len(&mut rd).unwrap();

    //     for _ in 0..meta_size {
    //         meta.insert(
    //             strings[read_usize(&mut rd).unwrap()].clone(),
    //             strings[read_usize(&mut rd).unwrap()].clone()
    //         );
    //     }

    //     let metrics_size = read_map_len(&mut rd).unwrap();

    //     for _ in 0..metrics_size {
    //         metrics.insert(
    //             strings[read_usize(&mut rd).unwrap()].clone(),
    //             read_f64(&mut rd).unwrap()
    //         );
    //     }

    //     (meta, metrics)
    // }

    fn from_str(&mut self, s: &str) -> Rc<str> {
        match self.strings.get(s) {
            Some(s) => s.clone(),
            None => {
                let s: Rc<str> = Rc::from(s);
                self.strings.insert(s.clone());
                s
            }
        }
    }
}
