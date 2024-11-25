// Copyright 2024-Present Datadog, Inc. https://www.datadoghq.com/
// SPDX-License-Identifier: Apache-2.0
use super::stacktrace::StackTrace;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ErrorData {
    pub is_crash: bool,
    pub kind: ErrorKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub source_type: SourceType,
    pub stack: StackTrace,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub threads: Vec<ThreadData>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub enum SourceType {
    Crashtracking,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[repr(C)]
pub enum ErrorKind {
    Panic,
    UnhandledException,
    UnixSignal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ThreadData {
    pub crashed: bool,
    pub name: String,
    pub stack: StackTrace,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
}

impl From<(String, Vec<crate::StackFrame>)> for ThreadData {
    fn from(value: (String, Vec<crate::StackFrame>)) -> Self {
        let crashed = false; // Currently, only .Net uses this, and I believe they don't put the crashing thread here
        let name = value.0;
        let stack = value.1.into();
        let state = None;
        Self {
            crashed,
            name,
            stack,
            state,
        }
    }
}

pub fn thread_data_from_additional_stacktraces(
    additional_stacktraces: HashMap<String, Vec<crate::StackFrame>>,
) -> Vec<ThreadData> {
    additional_stacktraces
        .into_iter()
        .map(|x| x.into())
        .collect()
}
