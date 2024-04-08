use crate::{tracing::Segments, config::Config, metadata::ProcessInfo};

pub mod agent;

pub trait Exporter {
    fn configure(&mut self, config: Config);
    fn export(&self, traces: Segments, process_info: &ProcessInfo);
}
