use serde::Serialize;
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize)]
pub struct UpdateSamplingRatesCommand {
    pub rate_by_service: HashMap<String, f32>
}

#[derive(Clone, Debug)]
pub enum Command {
    // Public commands
    UpdateSamplingRates(UpdateSamplingRatesCommand),

    // Private commands
}
