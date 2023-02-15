use ddtelemetry::trace::Agent;
use ddtelemetry::trace::{DefaultTraceFlusher, TraceFlusher};

struct ServerlessTraceFlusher {}

impl TraceFlusher for ServerlessTraceFlusher {
    fn flush(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Serverless trace flusher flushed");
        Ok(())
    }
}

fn main() {
    // let agent = Box::new(Agent {
    //     flusher: Box::new(DefaultTraceFlusher {}),
    // });

    let agent = Box::new(Agent {
        flusher: Box::new(ServerlessTraceFlusher {}),
    });

    agent.run();
}
