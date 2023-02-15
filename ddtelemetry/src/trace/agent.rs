use super::TraceFlusher;
use std::{thread, time};

pub struct Agent {
    pub flusher: Box<dyn TraceFlusher>,
}

impl Agent {
    pub fn run(&self) {
        loop {
            self.flusher.flush().expect("Error flushing");
            thread::sleep(time::Duration::from_millis(1000));
        }
    }
}
