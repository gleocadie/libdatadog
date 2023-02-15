use std::error::Error;

pub trait TraceFlusher {
    fn flush(&self) -> Result<(), Box<dyn Error>>;
}

pub struct DefaultTraceFlusher {
    
}

impl TraceFlusher for DefaultTraceFlusher {
    fn flush(&self) -> Result<(), Box<dyn Error>> {
        println!("Running default trace flusher");
        Ok(())
    }
}
