use std::io::Read;
use tokio::sync::{mpsc::{Sender, Receiver, self}, broadcast};

use crate::{runtime::RUNTIME, processing::Processor, encoding::msgpack::{MessagePackDecoder, MessagePackEncoder}};

pub struct Collector {
    tx: Sender<Vec<u8>>,
    ch: broadcast::Sender<Vec<u8>>
}

// TODO: Optimize reading to avoid conversion to a vector.
// TODO: Consider `bytes` crate to avoid cloning the underlying slice.
impl Collector {
    pub fn new() -> Self {
        let (tx, rx): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = mpsc::channel(8);
        let (ch, _): (broadcast::Sender<Vec<u8>>, _) = broadcast::channel(1000);
        let mut processor = Processor::new();

        Self::setup_encoding(&mut processor, ch.clone());
        Self::setup_decoding(processor, rx);

        Self { tx, ch }
    }

    pub fn write<R: Read>(&self, mut rd: R) {
        let tx = self.tx.clone();
        let mut buf = vec![];

        rd.read_to_end(&mut buf).unwrap();

        _ = tx.blocking_send(buf);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Vec<u8>> {
        self.ch.subscribe()
    }

    fn setup_encoding(processor: &mut Processor, tx: broadcast::Sender<Vec<u8>>) {
        let mut encoder = MessagePackEncoder::new();
        let mut encode_rx = encoder.subscribe();
        let mut rx = processor.subscribe();

        RUNTIME.spawn(async move {
            while let Ok(buffer) = encode_rx.recv().await {
                _ = tx.send(buffer);
            }
        });

        RUNTIME.spawn(async move {
            while let Ok(cmd) = rx.recv().await {
                encoder.encode(cmd);
            }
        });
    }

    fn setup_decoding(mut processor: Processor, mut rx: Receiver<Vec<u8>>) {
        let mut decoder = MessagePackDecoder::new();
        let mut decode_rx = decoder.subscribe();

        RUNTIME.spawn(async move {
            while let Ok(event) = decode_rx.recv().await {
                processor.process(event);
            }
        });

        RUNTIME.spawn(async move {
            while let Some(payload) = rx.recv().await {
                decoder.decode(payload.as_slice());
            }
        });
    }
}
