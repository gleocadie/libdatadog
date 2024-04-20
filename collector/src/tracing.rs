use std::collections::HashMap;
use std::sync::Arc as Rc;

pub type Meta = HashMap<Rc<str>, Rc<str>>;
pub type Metrics = HashMap<Rc<str>, f64>;
pub type Segments = HashMap<u64, Segment>;

#[derive(Debug)]
pub struct Span {
    pub span_type: Rc<str>,
    pub span_id: u64,
    pub parent_id: u64,
    pub name: Rc<str>,
    pub resource: Rc<str>,
    pub service: Rc<str>,
    pub error: u64,
    pub start: u64,
    pub duration: u64,
    pub meta: Meta,
    pub metrics: Metrics
}

#[derive(Debug)]
pub struct Segment {
    pub start: u64,
    pub trace_id: u128,
    pub started: u64,
    pub finished: u64,
    pub root: u64,
    pub spans: Vec<Span>
}
