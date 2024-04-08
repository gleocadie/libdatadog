use std::collections::HashMap;
use std::sync::Arc as Rc;

use crate::{config::Config, metadata::ProcessInfo};

#[derive(Clone, Debug)]
pub struct StartSegmentEvent {
    pub time: u64,
    pub trace_id: u128,
    pub segment_id: u64,
    pub parent_id: u64,
}

#[derive(Clone, Debug)]
pub struct FinishSegmentEvent {
    pub ticks: u64,
    pub segment_id: u64,
}

#[derive(Clone, Debug)]
pub struct StartSpanEvent {
    pub ticks: u64,
    pub segment_id: u64,
    pub span_id: u64,
    pub parent_id: u64,
    pub service: Rc<str>,
    pub name: Rc<str>,
    pub resource: Rc<str>,
    pub meta: HashMap<Rc<str>, Rc<str>>,
    pub metrics: HashMap<Rc<str>, f64>,
    pub span_type: Rc<str>,
}

#[derive(Clone, Debug)]
pub struct FinishSpanEvent {
    pub ticks: u64,
    pub segment_id: u64,
    pub span_id: u64,
}

#[derive(Clone, Debug)]
pub struct ExceptionEvent {
    pub segment_id: u64,
    pub span_id: u64,
    pub message: Rc<str>,
    pub name: Rc<str>,
    pub stack: Rc<str>,
}

#[derive(Clone, Debug)]
pub struct ErrorEvent {
    pub segment_id: u64,
    pub span_id: u64,
}

#[derive(Clone, Debug)]
pub struct AddTagsEvent {
    pub segment_id: u64,
    pub span_id: u64,
    pub meta: HashMap<Rc<str>, Rc<str>>,
    pub metrics: HashMap<Rc<str>, f64>,
}

#[derive(Clone, Debug)]
pub struct SamplingPriorityEvent {
    pub segment_id: u64,
    pub priority: i8,
    pub mechanism: i8,
    pub rate: f32,
}

#[derive(Clone, Debug)]
pub enum Event {
    // Public events
    StartSegment(StartSegmentEvent),
    FinishSegment(FinishSegmentEvent),
    StartSpan(StartSpanEvent),
    FinishSpan(FinishSpanEvent),
    Exception(ExceptionEvent),
    Error(ErrorEvent),
    AddTags(AddTagsEvent),
    Config(Config),
    ProcessInfo(ProcessInfo),
    SamplingPriority(SamplingPriorityEvent),

    // Private events
    FlushTraces,
}
