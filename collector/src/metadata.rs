use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct ProcessInfo {
    pub tracer_version: String,
    pub language: String,
    pub language_version: String,
    pub language_interpreter: String,
}
