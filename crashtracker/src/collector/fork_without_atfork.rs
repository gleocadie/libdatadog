// Copyright 2024-Present Datadog, Inc. https://www.datadoghq.com/
// SPDX-License-Identifier: Apache-2.0

use libc::pid_t;
use std::fs::File;
use std::io::{self, BufRead, BufReader};

#[cfg(target_os = "linux")]
fn is_being_traced() -> io::Result<bool> {
    // Check to see whether we are being traced.  This will fail on systems where procfs is
    // unavailable, but presumably in those systems `ptrace()` is also unavailable.
    let file = File::open("/proc/self/status")?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        let line = line?;
        if line.starts_with("TracerPid:") {
            let tracer_pid = line.split_whitespace().nth(1).unwrap_or("0");
            return Ok(tracer_pid != "0");
        }
    }

    Ok(false)
}

#[cfg(target_os = "linux")]
pub fn fork_without_atfork() -> pid_t {
    use libc::{
        c_ulong, c_void, syscall, SYS_clone, CLONE_CHILD_CLEARTID, CLONE_CHILD_SETTID,
        CLONE_PTRACE, SIGCHLD,
    };

    let mut ptid: pid_t = 0;
    let mut ctid: pid_t = 0;

    // Check whether we're traced before we fork.
    let being_traced = match is_being_traced() {
        Ok(being_traced) => being_traced,
        Err(_) => false,
    };
    let extra_flags = if being_traced { CLONE_PTRACE } else { 0 };

    // Use the direct syscall interface into `clone()`.  This should replicate the parameters used
    // for glibc `fork()`, except of course without calling the atfork handlers.
    // One question is whether we're using the right set of flags.  For instance, does suppressing
    // `SIGCHLD` here make it easier for us to handle some conditions in the parent process?
    let res = unsafe {
        syscall(
            SYS_clone,
            (CLONE_CHILD_CLEARTID | CLONE_CHILD_SETTID | SIGCHLD | extra_flags) as c_ulong,
            std::ptr::null_mut::<c_void>(),
            &mut ptid as *mut pid_t,
            &mut ctid as *mut pid_t,
            0 as c_ulong,
        )
    };

    if res == -1 {
        return -1;
    }

    ctid
}

#[cfg(target_os = "macos")]
pub fn fork_without_atfork() -> pid_t {
    use libc::c_long;
    use libc::syscall;

    const SYS_FORK: c_long = 2;

    let res = unsafe { syscall(SYS_FORK) };

    if res == -1 {
        // Handle error
        return -1;
    }

    res as pid_t
}
