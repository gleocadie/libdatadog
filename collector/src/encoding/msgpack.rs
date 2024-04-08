use std::io::Write;
use std::{io::Read, sync::Arc as Rc};
use std::collections::HashMap;
use rmp::decode::{read_array_len, read_f32, read_f64, read_int, read_map_len, read_str_len, NumValueReadError};
use tokio::sync::broadcast::{self, Sender, Receiver};

use crate::commands::{Command, UpdateSamplingRatesCommand};
use crate::events::*;
use crate::tracing::{Meta, Metrics};

pub struct Segment {
    id: u64,
    spans: Vec<u64>,
}

impl Segment {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            spans: Vec::new(),
        }
    }
}

pub struct MessagePackDecoder {
    tx: Sender<Event>,
    strings: Vec<Rc<str>>,
    segments: Vec<Segment>,
}

pub struct MessagePackEncoder {
    tx: Sender<Vec<u8>>,
}

impl MessagePackEncoder {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(1000);

        Self { tx }
    }

    pub fn subscribe(&mut self) -> Receiver<Vec<u8>> {
        self.tx.subscribe()
    }

    pub fn encode(&self, cmd: Command) {
        let mut buf = vec![];

        match cmd {
            Command::UpdateSamplingRates(cmd) => self.encode_sampling_rates(cmd, &mut buf),
        };

        self.tx.send(buf).unwrap();
    }

    fn encode_sampling_rates<W: Write>(&self, cmd: UpdateSamplingRatesCommand, buf: &mut W) {
        buf.write(&[1 as u8]).unwrap();
        rmp_serde::encode::write_named(buf, &cmd).unwrap();
    }
}

impl MessagePackDecoder {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(800);
        let default_segment = Segment::new(0);

        Self {
            strings: Vec::from([Rc::from("")]),
            segments: Vec::from([default_segment]),
            tx
        }
    }

    pub fn subscribe(&mut self) -> Receiver<Event> {
        self.tx.subscribe()
    }

    pub fn decode<R: Read>(&mut self, mut rd: R) {
        while let Ok(event_type) = read_int(&mut rd) {
            match event_type {
                -2 =>  self.decode_segments(&mut rd),
                -1 =>  self.decode_strings(&mut rd),
                0 => self.reset_stream(),
                128 => self.decode_join_session(&mut rd),
                129 => self.decode_process_info(&mut rd),
                130 => self.decode_start_segment(&mut rd),
                131 => self.decode_start_span(&mut rd),
                132 => self.decode_finish_span(&mut rd),
                133 => self.decode_add_tags(&mut rd),
                135 => self.decode_sampling_priority(&mut rd),
                136 => self.decode_exception(&mut rd),
                137 => self.decode_add_links(&mut rd),
                138 => self.decode_error(&mut rd),
                139 => self.decode_finish_segment(&mut rd),
                140 => self.decode_config(&mut rd),
                _ => (),
            };
        }

        // TODO: Don't trigger flush in the decoder.
        self.tx.send(Event::FlushTraces).unwrap();
        self.strings.truncate(1);
    }

    fn decode_strings<R: Read>(&mut self, mut rd: R) {
        let size = read_array_len(&mut rd).unwrap();

        self.strings.reserve(size as usize);

        for _ in 0..size {
            let s = self.read_str(&mut rd);
            self.strings.push(Rc::from(s));
        }
    }

    fn decode_segments<R: Read>(&mut self, mut rd: R) {
        let size = read_array_len(&mut rd).unwrap();

        for _ in 1..size {
            let id = read_int(&mut rd).unwrap();
            let segment = Segment::new(id);

            self.segments.push(segment)
        }
    }

    fn decode_join_session<R: Read>(&mut self, mut rd: R) {
        let _: u64 = read_int(&mut rd).unwrap();
    }

    fn decode_start_segment<R: Read>(&mut self, mut rd: R) {
        read_array_len(&mut rd).unwrap();

        let time = read_int(&mut rd).unwrap();
        let trace_id = self.read_trace_id(&mut rd).unwrap();
        let segment_index = self.read_index(&mut rd).unwrap();
        let parent_id = self.read_span_id(&mut rd).unwrap();

        let segment = self.segments.get_mut(segment_index).unwrap();
        let event = Event::StartSegment(StartSegmentEvent {
            time,
            trace_id,
            segment_id: segment.id,
            parent_id,
        });

        segment.spans.push(parent_id);

        self.tx.send(event).unwrap();
    }

    fn decode_finish_segment<R: Read>(&mut self, mut rd: R) {
        read_array_len(&mut rd).unwrap();

        let ticks = read_int(&mut rd).unwrap();
        let segment = self.get_segment(&mut rd).unwrap();

        let event = Event::FinishSegment(FinishSegmentEvent {
            ticks,
            segment_id: segment.id,
        });

        self.tx.send(event).unwrap();
    }

    fn decode_exception<R: Read>(&mut self, mut rd: R) {
        read_array_len(&mut rd).unwrap();

        let segment = self.get_segment(&mut rd).unwrap();
        let span_id = self.get_span_id(segment, &mut rd).unwrap();
        let message = self.strings[self.read_index(&mut rd).unwrap()].clone();
        let name = self.strings[self.read_index(&mut rd).unwrap()].clone();
        let stack = self.strings[self.read_index(&mut rd).unwrap()].clone();

        let event = Event::Exception(ExceptionEvent {
            segment_id: segment.id,
            span_id,
            message,
            name,
            stack,
        });

        self.tx.send(event).unwrap();
    }

    fn decode_add_links<R: Read>(&mut self, _rd: R) {

    }

    fn decode_error<R: Read>(&mut self, mut rd: R) {
        read_array_len(&mut rd).unwrap();

        let segment = self.get_segment(&mut rd).unwrap();
        let span_id = self.get_span_id(segment, &mut rd).unwrap();

        let event = Event::Error(ErrorEvent {
            segment_id: segment.id,
            span_id,
        });

        self.tx.send(event).unwrap();
    }

    fn decode_start_span<R: Read>(&mut self, mut rd: R) {
        read_array_len(&mut rd).unwrap();

        let ticks = read_int(&mut rd).unwrap();
        let segment = self.get_segment(&mut rd).unwrap();
        let span_id = self.read_span_id(&mut rd).unwrap();
        let parent_id = self.get_span_id(segment, &mut rd).unwrap();
        let service = self.strings[self.read_index(&mut rd).unwrap()].clone();
        let name = self.strings[self.read_index(&mut rd).unwrap()].clone();
        let resource = self.strings[self.read_index(&mut rd).unwrap()].clone();
        let (meta, metrics) = self.read_tags(&mut rd, &self.strings);
        let span_type = self.strings[self.read_index(&mut rd).unwrap()].clone();

        let event = Event::StartSpan(StartSpanEvent {
            ticks,
            segment_id: segment.id,
            span_id,
            parent_id,
            service,
            name,
            resource,
            meta,
            metrics,
            span_type,
        });

        self.tx.send(event).unwrap();
    }

    fn decode_finish_span<R: Read>(&mut self, mut rd: R) {
        read_array_len(&mut rd).unwrap();

        let ticks = read_int(&mut rd).unwrap();
        let segment = self.get_segment(&mut rd).unwrap();
        let span_id = self.get_span_id(segment, &mut rd).unwrap();

        let event = Event::FinishSpan(FinishSpanEvent {
            ticks,
            segment_id: segment.id,
            span_id,
        });

        self.tx.send(event).unwrap();
    }

    fn decode_add_tags<R: Read>(&mut self, mut rd: R) {
        read_array_len(&mut rd).unwrap();

        let segment = self.get_segment(&mut rd).unwrap();
        let span_id = self.get_span_id(segment, &mut rd).unwrap();
        let (meta, metrics) = self.read_tags(&mut rd, &self.strings);

        let event = Event::AddTags(AddTagsEvent {
            segment_id: segment.id,
            span_id,
            meta,
            metrics,
        });

        self.tx.send(event).unwrap();
    }

    fn decode_sampling_priority<R: Read>(&mut self, mut rd: R) {
        read_array_len(&mut rd).unwrap();

        let segment = self.get_segment(&mut rd).unwrap();
        let priority = read_int(&mut rd).unwrap();
        let mechanism = read_int(&mut rd).unwrap();
        let rate = read_f32(&mut rd).unwrap();

        let event = Event::SamplingPriority(SamplingPriorityEvent {
            segment_id: segment.id,
            priority,
            mechanism,
            rate,
        });

        self.tx.send(event).unwrap();
    }

    fn decode_config<R: Read>(&mut self, mut rd: R) {
        let config = rmp_serde::from_read(&mut rd).unwrap();
        let event = Event::Config(config);

        self.tx.send(event).unwrap();
    }

    fn decode_process_info<R: Read>(&mut self, mut rd: R) {
        let info = rmp_serde::from_read(&mut rd).unwrap();
        let event = Event::ProcessInfo(info);

        self.tx.send(event).unwrap();
    }

    fn read_index<R: Read>(&self, mut rd: R) -> Result<usize, NumValueReadError> {
        read_int(&mut rd)
    }

    fn read_trace_id<R: Read>(&self, mut rd: R) -> Result<u128, NumValueReadError> {
        let len = rmp::decode::read_bin_len(&mut rd)?;

        match len {
            16 => self.read_data_u128(&mut rd),
            8 => Ok(self.read_data_u64(&mut rd)? as u128),
            _ => Ok(0),
        }
    }

    fn read_span_id<R: Read>(&self, mut rd: R) -> Result<u64, NumValueReadError> {
        let len = rmp::decode::read_bin_len(&mut rd)?;

        match len {
            8 => self.read_data_u64(&mut rd),
            _ => Ok(0),
        }
    }

    fn read_data_u128<R: Read>(&self, mut rd: R) -> Result<u128, NumValueReadError>{
        let mut buf = [0; 16];
        let _ = rd.read_exact(&mut buf);

        Ok(u128::from_be_bytes(buf))
    }

    fn read_data_u64<R: Read>(&self, mut rd: R) -> Result<u64, NumValueReadError>{
        let mut buf = [0; 8];
        let _ = rd.read_exact(&mut buf);

        Ok(u64::from_be_bytes(buf))
    }

    fn read_str<R: Read>(&self, mut rd: R) -> String {
        let limit = read_str_len(&mut rd).unwrap() as u64;
        let mut str = String::new();

        rd.by_ref().take(limit).read_to_string(&mut str).unwrap();

        str
    }

    fn read_tags<R: Read>(&self, mut rd: R, strings: &[Rc<str>]) -> (Meta, Metrics){
        let mut meta = HashMap::new();
        let mut metrics = HashMap::new();

        let meta_size = read_map_len(&mut rd).unwrap();

        for _ in 0..meta_size {
            meta.insert(
                strings[self.read_index(&mut rd).unwrap()].clone(),
                strings[self.read_index(&mut rd).unwrap()].clone()
            );
        }

        let metrics_size = read_map_len(&mut rd).unwrap();

        for _ in 0..metrics_size {
            metrics.insert(
                strings[self.read_index(&mut rd).unwrap()].clone(),
                read_f64(&mut rd).unwrap()
            );
        }

        (meta, metrics)
    }

    fn get_segment<R: Read>(&self, mut rd: R) -> Result<&Segment, NumValueReadError> {
        let segment_index = self.read_index(&mut rd)?;
        let segment = self.segments.get(segment_index).unwrap();

        Ok(segment)
    }

    fn get_span_id<R: Read>(&self, segment: &Segment, mut rd: R) -> Result<u64, NumValueReadError> {
        let span_index = self.read_index(&mut rd)?;
        let span_id = segment.spans.get(span_index).unwrap_or(&0);

        Ok(*span_id)
    }

    fn reset_stream(&mut self) {

    }
}
