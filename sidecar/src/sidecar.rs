use std::{
    os::{fd::AsRawFd, unix::net::UnixListener as StdUnixListener},
    path::PathBuf, fs::OpenOptions,
};

use ddtelemetry::ipc::setup::Liaison;
use spawn_worker::{entrypoint, Stdio};
use sysinfo::{System, SystemExt, ProcessExt};
use tokio::net::UnixListener;

use std::io::Write;

use crate::mini_agent;

#[no_mangle]
pub extern "C" fn sidecar_entrypoint() {
    if let Some(fd) = spawn_worker::recv_passed_fd() {
        let listener = StdUnixListener::try_from(fd).unwrap();

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let _rt_guard = rt.enter();
        listener.set_nonblocking(true).unwrap();
        let listener = UnixListener::from_std(listener).unwrap();

        let server_future = mini_agent::main(listener);

        rt.block_on(server_future).unwrap();
    }
}

#[allow(dead_code)]
pub(crate) unsafe fn maybe_start() -> anyhow::Result<PathBuf> {
    // println!("in maybe start");
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open("/tmp/mini-agent-logs.txt")
        .unwrap();

    writeln!(f, "in maybe start").unwrap();

    let liaison = ddtelemetry::ipc::setup::SharedDirLiaison::new_tmp_dir();
    if let Some(listener) = liaison.attempt_listen()? {
        let child_pid = spawn_worker::SpawnWorker::new()
            .stdin(Stdio::Null)
            .stderr(Stdio::Inherit)
            .stdout(Stdio::Inherit)
            .pass_fd(listener)
            .daemonize(true)
            .target(entrypoint!(sidecar_entrypoint))
            .spawn()?;
        writeln!(f, "spawned child pid in maybe_start: {:?}", child_pid.pid).unwrap();
    };

    let process_name: String = std::env::current_exe()
        .ok()
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();

    writeln!(f, "|current process name in maybe_start: {}|", process_name).unwrap();
    writeln!(f, "|current process pid in maybe_start: {}|", std::process::id()).unwrap();

    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open("/tmp/mini-agent-logs.txt")
        .unwrap();

    let s = System::new_all();
    
    writeln!(f, "printing processes in maybe_start after spawn...").unwrap();
    for (pid, process) in s.processes() {
        writeln!(f, "process: {} {} {} {:?}", pid, process.exe().to_string_lossy(), process.name(), process.status()).unwrap();
    }

    // TODO: temporary hack - connect to socket and leak it
    // this should lead to sidecar being up as long as the processes that attempted to connect to it

    let con = liaison.connect_to_server()?;
    nix::unistd::dup(con.as_raw_fd())?; // LEAK! - dup also resets (?) CLOEXEC flag set by Rust UnixStream constructor

    Ok(liaison.path().to_path_buf())
}
