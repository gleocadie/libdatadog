// Copyright 2023-Present Datadog, Inc. https://www.datadoghq.com/
// SPDX-License-Identifier: Apache-2.0
#![cfg(unix)]

use super::*;
use anyhow::Context;
use std::{io::BufReader, os::unix::net::UnixListener};

pub fn resolve_frames(
    config: &CrashtrackerConfiguration,
    crash_info: &mut CrashInfo,
) -> anyhow::Result<()> {
    if config.resolve_frames == StacktraceCollection::EnabledWithSymbolsInReceiver {
        let proc_info = crash_info
            .proc_info
            .as_ref()
            .context("Unable to resolve frames: No PID specified")?;
        crash_info.resolve_names_from_process(proc_info.pid)?
    }
    Ok(())
}

pub fn get_unix_socket(socket_path: impl AsRef<str>) -> anyhow::Result<UnixListener> {
    let socket_path = socket_path.as_ref();
    if std::fs::metadata(socket_path).is_ok() {
        std::fs::remove_file(socket_path)
            .with_context(|| format!("could not delete previous socket at {:?}", socket_path))?;
    }

    let unix_listener =
        UnixListener::bind(socket_path).context("Could not create the unix socket")?;
    Ok(unix_listener)
}

pub fn reciever_entry_point_unix_socket(socket_path: impl AsRef<str>) -> anyhow::Result<()> {
    let listener = get_unix_socket(socket_path)?;
    let (unix_stream, _) = listener.accept()?;
    let stream = BufReader::new(unix_stream);
    receiver_entry_point(stream)
    // Dropping the stream closes it, allowing the collector to exit if it was waiting.
}

pub fn receiver_entry_point_stdin() -> anyhow::Result<()> {
    let stream = std::io::stdin().lock();
    receiver_entry_point(stream)
}

/// Receives data from a crash collector via a pipe on `stdin`, formats it into
/// `CrashInfo` json, and emits it to the endpoint/file defined in `config`.
///
/// At a high-level, this exists because doing anything in a
/// signal handler is dangerous, so we fork a sidecar to do the stuff we aren't
/// allowed to do in the handler.
///
/// See comments in [profiling/crashtracker/mod.rs] for a full architecture
/// description.
fn receiver_entry_point(stream: impl std::io::BufRead) -> anyhow::Result<()> {
    match receive_report(stream)? {
        CrashReportStatus::NoCrash => Ok(()),
        CrashReportStatus::CrashReport(config, mut crash_info) => {
            resolve_frames(&config, &mut crash_info)?;
            crash_info.upload_to_endpoint(&config)
        }
        CrashReportStatus::PartialCrashReport(config, mut crash_info, stdin_state) => {
            eprintln!("Failed to fully receive crash.  Exit state was: {stdin_state:?}");
            resolve_frames(&config, &mut crash_info)?;
            crash_info.upload_to_endpoint(&config)
        }
    }
}

/// The crashtracker collector sends data in blocks.
/// This enum tracks which block we're currently in, and, for multi-line blocks,
/// collects the partial data until the block is closed and it can be appended
/// to the CrashReport.
#[derive(Debug)]
enum StdinState {
    Config,
    Counters,
    Done,
    File(String, Vec<String>),
    InternalError(String),
    Metadata,
    ProcInfo,
    SigInfo,
    SpanIds,
    StackTrace(Vec<StackFrame>),
    TraceIds,
    Waiting,
}

/// A state machine that processes data from the crash-tracker collector line by
/// line.  The crashtracker collector sends data in blocks, so we use a `state`
/// variable to track which block we're in and collect partial data.
/// Once we reach the end of a block, append the block's data to `crashinfo`.
fn process_line(
    crashinfo: &mut CrashInfo,
    config: &mut Option<CrashtrackerConfiguration>,
    line: String,
    state: StdinState,
) -> anyhow::Result<StdinState> {
    let next = match state {
        StdinState::Config if line.starts_with(DD_CRASHTRACK_END_CONFIG) => StdinState::Waiting,
        StdinState::Config => {
            if config.is_some() {
                // The config might contain sensitive data, don't log it.
                eprintln!("Unexpected double config");
            }
            std::mem::swap(config, &mut Some(serde_json::from_str(&line)?));
            StdinState::Config
        }

        StdinState::Counters if line.starts_with(DD_CRASHTRACK_END_COUNTERS) => StdinState::Waiting,
        StdinState::Counters => {
            let v: serde_json::Value = serde_json::from_str(&line)?;
            let map = v.as_object().context("Expected map type value")?;
            anyhow::ensure!(map.len() == 1);
            let (key, val) = map
                .iter()
                .next()
                .context("we know there is one value here")?;
            let val = val.as_i64().context("Vals are ints")?;
            crashinfo.add_counter(key, val)?;
            StdinState::Counters
        }

        StdinState::Done => {
            eprintln!("Unexpected line after crashreport is done: {line}");
            StdinState::Done
        }

        StdinState::File(filename, lines) if line.starts_with(DD_CRASHTRACK_END_FILE) => {
            crashinfo.add_file_with_contents(&filename, lines)?;
            StdinState::Waiting
        }
        StdinState::File(name, mut contents) => {
            contents.push(line);
            StdinState::File(name, contents)
        }

        StdinState::InternalError(e) => anyhow::bail!("Can't continue after internal error {e}"),

        StdinState::Metadata if line.starts_with(DD_CRASHTRACK_END_METADATA) => StdinState::Waiting,
        StdinState::Metadata => {
            let metadata = serde_json::from_str(&line)?;
            crashinfo.set_metadata(metadata)?;
            StdinState::Metadata
        }

        StdinState::ProcInfo if line.starts_with(DD_CRASHTRACK_END_PROCINFO) => StdinState::Waiting,
        StdinState::ProcInfo => {
            let proc_info = serde_json::from_str(&line)?;
            crashinfo.set_procinfo(proc_info)?;
            StdinState::ProcInfo
        }

        StdinState::SigInfo if line.starts_with(DD_CRASHTRACK_END_SIGINFO) => StdinState::Waiting,
        StdinState::SigInfo => {
            let siginfo = serde_json::from_str(&line)?;
            crashinfo.set_siginfo(siginfo)?;
            crashinfo.set_timestamp_to_now()?;
            StdinState::SigInfo
        }

        StdinState::SpanIds if line.starts_with(DD_CRASHTRACK_END_SPAN_IDS) => StdinState::Waiting,
        StdinState::SpanIds => {
            let v: Vec<u128> = serde_json::from_str(&line)?;
            crashinfo.set_span_ids(v)?;
            StdinState::SpanIds
        }

        StdinState::StackTrace(stacktrace) if line.starts_with(DD_CRASHTRACK_END_STACKTRACE) => {
            crashinfo.set_stacktrace(None, stacktrace)?;
            StdinState::Waiting
        }
        StdinState::StackTrace(mut stacktrace) => {
            let frame = serde_json::from_str(&line).context(line)?;
            stacktrace.push(frame);
            StdinState::StackTrace(stacktrace)
        }

        StdinState::TraceIds if line.starts_with(DD_CRASHTRACK_END_TRACE_IDS) => {
            StdinState::Waiting
        }
        StdinState::TraceIds => {
            let v: Vec<u128> = serde_json::from_str(&line)?;
            crashinfo.set_trace_ids(v)?;
            StdinState::TraceIds
        }

        StdinState::Waiting if line.starts_with(DD_CRASHTRACK_BEGIN_CONFIG) => StdinState::Config,
        StdinState::Waiting if line.starts_with(DD_CRASHTRACK_BEGIN_COUNTERS) => {
            StdinState::Counters
        }
        StdinState::Waiting if line.starts_with(DD_CRASHTRACK_BEGIN_FILE) => {
            let (_, filename) = line.split_once(' ').unwrap_or(("", "MISSING_FILENAME"));
            StdinState::File(filename.to_string(), vec![])
        }
        StdinState::Waiting if line.starts_with(DD_CRASHTRACK_BEGIN_METADATA) => {
            StdinState::Metadata
        }
        StdinState::Waiting if line.starts_with(DD_CRASHTRACK_BEGIN_PROCINFO) => {
            StdinState::ProcInfo
        }
        StdinState::Waiting if line.starts_with(DD_CRASHTRACK_BEGIN_SIGINFO) => StdinState::SigInfo,
        StdinState::Waiting if line.starts_with(DD_CRASHTRACK_BEGIN_SPAN_IDS) => {
            StdinState::SpanIds
        }
        StdinState::Waiting if line.starts_with(DD_CRASHTRACK_BEGIN_STACKTRACE) => {
            StdinState::StackTrace(vec![])
        }
        StdinState::Waiting if line.starts_with(DD_CRASHTRACK_BEGIN_TRACE_IDS) => {
            StdinState::TraceIds
        }
        StdinState::Waiting if line.starts_with(DD_CRASHTRACK_DONE) => StdinState::Done,
        StdinState::Waiting => {
            //TODO: Do something here?
            eprintln!("Unexpected line while receiving crashreport: {line}");
            StdinState::Waiting
        }
    };
    Ok(next)
}

enum CrashReportStatus {
    NoCrash,
    CrashReport(CrashtrackerConfiguration, CrashInfo),
    PartialCrashReport(CrashtrackerConfiguration, CrashInfo, StdinState),
}

/// Listens to `stream`, reading it line by line, until
/// 1. A crash-report is received, in which case it is processed for upload
/// 2. `stdin` closes without a crash report (i.e. if the parent terminated normally)
/// In the case where the parent failed to transfer a full crash-report
/// (for instance if it crashed while calculating the crash-report), we return
/// a PartialCrashReport.
fn receive_report(stream: impl std::io::BufRead) -> anyhow::Result<CrashReportStatus> {
    let mut crashinfo = CrashInfo::new();
    let mut stdin_state = StdinState::Waiting;
    let mut config = None;

    //TODO: This assumes that the input is valid UTF-8.
    for line in stream.lines() {
        let line = line?;
        match process_line(&mut crashinfo, &mut config, line, stdin_state) {
            Ok(next_state) => stdin_state = next_state,
            Err(e) => {
                // If the input is corrupted, stop and salvage what we can
                stdin_state = StdinState::InternalError(e.to_string());
                break;
            }
        }
    }

    if !crashinfo.crash_seen() {
        return Ok(CrashReportStatus::NoCrash);
    }

    let config = config.context("Missing crashtracker configuration")?;
    for filename in &config.additional_files {
        crashinfo
            .add_file(filename)
            .unwrap_or_else(|e| eprintln!("Unable to add file {filename}: {e}"));
    }

    // If we were waiting for data when stdin closed, let our caller know that
    // we only have partial data.
    if matches!(stdin_state, StdinState::Done) {
        Ok(CrashReportStatus::CrashReport(config, crashinfo))
    } else {
        crashinfo.set_incomplete(true)?;
        Ok(CrashReportStatus::PartialCrashReport(
            config,
            crashinfo,
            stdin_state,
        ))
    }
}
