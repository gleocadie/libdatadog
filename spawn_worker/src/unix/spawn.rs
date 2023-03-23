// Unless explicitly stated otherwise all files in this repository are licensed under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021-Present Datadog, Inc.

#[cfg(target_os = "linux")]
mod linux {
    use std::{io::{Seek, Write}, fs::OpenOptions};

    pub(crate) fn write_trampoline() -> anyhow::Result<memfd::Memfd> {
        let opts = memfd::MemfdOptions::default();
        let mfd = opts.create("spawn_worker_trampoline")?;

        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open("/tmp/mini-agent-logs.txt")
            .unwrap();

        writeln!(f, "in write_trampoline|").unwrap();
        writeln!(f, "trampoline bin length: {}|", crate::trampoline::TRAMPOLINE_BIN.len() as u64).unwrap();

        mfd.as_file()
            .set_len(crate::trampoline::TRAMPOLINE_BIN.len() as u64)?;
        mfd.as_file().write_all(crate::trampoline::TRAMPOLINE_BIN)?;
        mfd.as_file().rewind()?;

        Ok(mfd)
    }
}
use std::fs::{File, OpenOptions};

use std::{
    env,
    ffi::{self, CString, OsString},
    fs::Permissions,
    io::{Seek, Write},
    os::unix::prelude::{AsRawFd, OsStringExt, PermissionsExt},
};

use io_lifetimes::OwnedFd;

use nix::{sys::wait::WaitStatus, unistd::Pid};
use sysinfo::{System, SystemExt, ProcessExt};

use crate::fork::{fork, Fork};
use crate::utils::ExecVec;
use nix::libc;

fn write_to_tmp_file(data: &[u8]) -> anyhow::Result<tempfile::NamedTempFile> {
    let tmp_file = tempfile::NamedTempFile::new()?;
    let mut file = tmp_file.as_file();
    file.set_len(data.len() as u64)?;
    file.write_all(data)?;
    file.rewind()?;

    std::fs::set_permissions(tmp_file.path(), Permissions::from_mode(0o700))?;

    Ok(tmp_file)
}

#[derive(Clone, Debug)]
pub enum SpawnMethod {
    #[cfg(target_os = "linux")]
    FdExecTrampoline,
    #[cfg(not(target_os = "macos"))]
    LdPreloadTrampoline,
    ExecTrampoline,
}

pub enum Target {
    Entrypoint(crate::trampoline::Entrypoint),
    Manual(CString, CString),
    Noop,
}

impl Target {
    /// TODO: ld_preload type trampoline is not yet supported on osx
    /// loading executables as shared libraries with dlload + dlsym however seems to work ok?
    #[cfg(target_os = "macos")]
    pub fn detect_spawn_method(&self) -> std::io::Result<SpawnMethod> {
        Ok(SpawnMethod::Exec)
    }

    /// Automatically detect which spawn method should be used
    #[cfg(not(target_os = "macos"))]
    pub fn detect_spawn_method(&self) -> std::io::Result<SpawnMethod> {
        use std::path::PathBuf;
        let current_exec_path = env::current_exe()?;
        let current_exec_filename = current_exec_path.file_name().unwrap_or_default();
        #[cfg(target_os = "linux")]
        let default_method = SpawnMethod::FdExecTrampoline;

        #[cfg(not(target_os = "linux"))]
        let default_method = SpawnMethod::Exec;

        let target_path = match self {
            Target::Entrypoint(e) => e.get_fs_path().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "can't find the entrypoint's target path",
                )
            }),
            Target::Manual(p, _) => p
                .to_str()
                .map(PathBuf::from)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
            Target::Noop => return Ok(default_method),
        }?;
        let target_filename = target_path.file_name().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                "can't extract actual target filename",
            )
        })?;

        // simple heuristic that should cover most cases
        // if both executable path and target's entrypoint path end up having the same filenames
        // then it means its not a shared library - and we need to load the trampoline using ld_preload
        if current_exec_filename == target_filename {
            Ok(SpawnMethod::LdPreloadTrampoline)
        } else {
            Ok(default_method)
        }
    }
}

enum ChildStdio {
    Inherit,
    Owned(OwnedFd),
    Ref(libc::pid_t),
}

impl ChildStdio {
    fn as_fd(&self) -> Option<libc::pid_t> {
        match self {
            ChildStdio::Inherit => None,
            ChildStdio::Owned(fd) => Some(fd.as_raw_fd()),
            ChildStdio::Ref(fd) => Some(*fd),
        }
    }
}

pub enum Stdio {
    Inherit,
    Fd(OwnedFd),
    Null,
}

impl Stdio {
    fn as_child_stdio(&self) -> std::io::Result<ChildStdio> {
        match self {
            Stdio::Inherit => Ok(ChildStdio::Inherit),
            Stdio::Fd(fd) => {
                if fd.as_raw_fd() >= 0 && fd.as_raw_fd() <= libc::STDERR_FILENO {
                    Ok(ChildStdio::Owned(fd.try_clone()?))
                } else {
                    Ok(ChildStdio::Ref(fd.as_raw_fd()))
                }
            }
            Stdio::Null => {
                let dev_null = File::options().read(true).write(true).open("/dev/null")?;
                Ok(ChildStdio::Owned(dev_null.into()))
            }
        }
    }
}

impl From<File> for Stdio {
    fn from(val: File) -> Self {
        Stdio::Fd(val.into())
    }
}

pub struct SpawnWorker {
    stdin: Stdio,
    stderr: Stdio,
    stdout: Stdio,
    daemonize: bool,
    spawn_method: Option<SpawnMethod>,
    fd_to_pass: Option<OwnedFd>,
    target: Target,
    env: Vec<(ffi::OsString, ffi::OsString)>,
    process_name: Option<String>,
}

impl SpawnWorker {
    pub fn from_env<E: IntoIterator<Item = (ffi::OsString, ffi::OsString)>>(env: E) -> Self {
        Self {
            stdin: Stdio::Inherit,
            stdout: Stdio::Inherit,
            stderr: Stdio::Inherit,
            daemonize: false,
            target: Target::Noop,
            spawn_method: None,
            fd_to_pass: None,
            env: env.into_iter().collect(),
            process_name: None,
        }
    }

    /// # Safety
    /// since the rust library code can coexist with other code written in other languages
    /// access to environment (required to be read to be passed to subprocess) is unsafe
    ///
    /// ensure no other threads read the environment at the same time as this method is called
    pub unsafe fn new() -> Self {
        Self::from_env(env::vars_os())
    }

    pub fn target<T: Into<Target>>(&mut self, target: T) -> &mut Self {
        // println!("in spawn_worker target");
        self.target = target.into();
        self
    }

    pub fn process_name<S: Into<String>>(&mut self, process_name: S) -> &mut Self {
        self.process_name = Some(process_name.into());
        self
    }

    pub fn stdin<S: Into<Stdio>>(&mut self, stdio: S) -> &mut Self {
        self.stdin = stdio.into();
        self
    }

    pub fn stdout<S: Into<Stdio>>(&mut self, stdio: S) -> &mut Self {
        self.stdout = stdio.into();
        self
    }

    pub fn daemonize(&mut self, daemonize: bool) -> &mut Self {
        self.daemonize = daemonize;
        self
    }

    pub fn stderr<S: Into<Stdio>>(&mut self, stdio: S) -> &mut Self {
        self.stderr = stdio.into();
        self
    }

    pub fn spawn_method(&mut self, spawn_method: SpawnMethod) -> &mut Self {
        self.spawn_method = Some(spawn_method);
        self
    }

    pub fn pass_fd<T: Into<OwnedFd>>(&mut self, fd: T) -> &mut Self {
        self.fd_to_pass = Some(fd.into());
        self
    }

    pub fn append_env<K: Into<OsString>, V: Into<OsString>>(
        &mut self,
        key: K,
        value: V,
    ) -> &mut Self {
        self.env.push((key.into(), value.into()));
        self
    }

    pub fn spawn(&mut self) -> anyhow::Result<Child> {
        // println!("trying to spawn in spawn_worker");
        let pid = self.do_spawn()?;

        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open("/tmp/mini-agent-logs.txt")
            .unwrap();

        writeln!(f, "returning the following pid from spawn: {:?}|", pid).unwrap();
        
        Ok(Child { pid })
    }

    fn do_spawn(&self) -> anyhow::Result<Option<libc::pid_t>> {
        // println!("in do_spawn");

        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open("/tmp/mini-agent-logs.txt")
            .unwrap();

        println!("in do spawn|");
        writeln!(f, "in do spawn|").unwrap();

        let mut argv = ExecVec::<0>::empty();
        let mut new_argv = ExecVec::<0>::empty();
        // set argv[0] and process name shown eg in `ps`
        let process_name = CString::new(self.process_name.as_deref().unwrap_or("spawned_worker"))?;
        argv.push_cstring(process_name.clone());
        new_argv.push_cstring(process_name.clone());

        match &self.target {
            Target::Entrypoint(entrypoint) => {
                let path = match unsafe {
                    crate::utils::get_dl_path_raw(entrypoint.ptr as *const libc::c_void)
                } {
                    (Some(path), _) => path,
                    _ => return Err(anyhow::format_err!("can't read symbol pointer data")),
                };

                argv.push_cstring(path.clone());
                argv.push_cstring(entrypoint.symbol_name.clone());

                let lib_path = env::var("DD_MINI_AGENT_LIB_PATH").unwrap();
                // new_argv.push_cstring(CString::new("/home/bits/test/c_test/libsidecar.so").unwrap());
                new_argv.push_cstring(CString::new(lib_path).unwrap());
                new_argv.push_cstring(entrypoint.symbol_name.clone());
            }
            Target::Manual(path, symbol_name) => {
                argv.push_cstring(path.clone());
                argv.push_cstring(symbol_name.clone());
            }
            Target::Noop => return Ok(None),
        };

        let mut envp = ExecVec::<0>::empty();
        for (k, v) in &self.env {
            // reserve space for '=' and final null
            let mut env_entry = OsString::with_capacity(k.len() + v.len() + 2);
            env_entry.push(k);
            env_entry.reserve(v.len() + 2);
            env_entry.push("=");
            env_entry.push(v);

            if let Ok(env_entry) = CString::new(env_entry.into_vec()) {
                envp.push_cstring(env_entry);
            }
        }

        // setup arbitrary fd passing
        let _shorter_lived_fd = if let Some(src_fd) = &self.fd_to_pass {
            // we're stripping the close on exec flag from the FD
            // to ensure we will not modify original fd, whose expected lifetime is unknown
            // we should clone the FD that needs passing to the subprocess, keeping its lifetime
            // as short as possible

            // rationale: some FDs are more important than others
            //      e.g. listening socket fd must not be accidentally leaked to a subprocess
            //      this would cause hard to debug bugs where random process could block the address
            //  TODO: this method is not perfect, ideally we should create an anonymous socket pair
            //        then send any FDs through that socket pair. Ensuring no random spawned processes could leak
            let fd = src_fd.try_clone()?;
            envp.push_cstring(CString::new(format!(
                "{}={}",
                crate::ENV_PASS_FD_KEY,
                fd.as_raw_fd()
            ))?);
            let flags = nix::fcntl::fcntl(fd.as_raw_fd(), nix::fcntl::F_GETFD)?;
            unsafe {
                libc::fcntl(
                    fd.as_raw_fd(),
                    libc::F_SETFD,
                    flags & !nix::libc::FD_CLOEXEC,
                )
            };
            Some(fd) // keep the temporary fd in scope for the duration of this method
        } else {
            None
        };

        // setup final spawn
        let spawn_method = match &self.spawn_method {
            Some(m) => m.clone(),
            None => self.target.detect_spawn_method()?,
        };

        writeln!(f, "spawn method: {:?}|", spawn_method).unwrap();

        // build and allocate final exec fn and its dependencies
        let spawn: Box<dyn Fn()> = match spawn_method {
            #[cfg(target_os = "linux")]
            SpawnMethod::FdExecTrampoline => {
                let fd = linux::write_trampoline()?;
                Box::new(move || {
                    let mut f = OpenOptions::new()
                        .write(true)
                        .create(true)
                        .append(true)
                        .open("/tmp/mini-agent-logs.txt")
                        .unwrap();
                    // not using nix crate here, as it would allocate args after fork, which will lead to crashes on systems
                    // where allocator is not fork+thread safe
                    writeln!(f, "spawn method is being set|").unwrap();

                    writeln!(f, "about to call fexecve").unwrap();

                    unsafe { libc::fexecve(fd.as_raw_fd(), new_argv.as_ptr(), envp.as_ptr()) };

                    // if we're here then exec has failed
                    writeln!(f, "if we're here then exec has failed: {}", std::io::Error::last_os_error()).unwrap();
                    panic!("{}", std::io::Error::last_os_error());
                })
            }
            #[cfg(not(target_os = "macos"))]
            SpawnMethod::LdPreloadTrampoline => {
                let lib_path = write_to_tmp_file(crate::trampoline::LD_PRELOAD_TRAMPOLINE_LIB)?
                    .into_temp_path()
                    .keep()?;
                let env_prefix = "LD_PRELOAD=";

                let mut ld_env =
                    OsString::with_capacity(env_prefix.len() + lib_path.as_os_str().len() + 1);

                ld_env.push(env_prefix);
                ld_env.push(lib_path);
                envp.push_cstring(CString::new(ld_env.into_vec())?);

                let path = CString::new(env::current_exe()?.to_str().ok_or_else(|| {
                    anyhow::format_err!("can't convert current executable file to correct path")
                })?)?;

                Box::new(move || unsafe {
                    libc::execve(path.as_ptr(), argv.as_ptr(), envp.as_ptr());
                    // if we're here then exec has failed
                    panic!("{}", std::io::Error::last_os_error());
                })
            }
            SpawnMethod::ExecTrampoline => {
                let path = CString::new(
                    write_to_tmp_file(crate::trampoline::TRAMPOLINE_BIN)?
                        .into_temp_path()
                        .keep()? // ensure the file is not auto cleaned in parent process
                        .as_os_str()
                        .to_str()
                        .ok_or_else(|| anyhow::format_err!("can't convert tmp file path"))?,
                )?;

                Box::new(move || {
                    // not using nix crate here, to avoid allocations post fork
                    unsafe { libc::execve(path.as_ptr(), argv.as_ptr(), envp.as_ptr()) };
                    // if we're here then exec has failed
                    panic!("{}", std::io::Error::last_os_error());
                })
            }
        };
        let stdin = self.stdin.as_child_stdio()?;
        let stdout = self.stdout.as_child_stdio()?;
        let stderr = self.stderr.as_child_stdio()?;

        // no allocations in the child process should happen by this point for maximum safety
        if let Fork::Parent(child_pid) = unsafe { fork()? } {
            writeln!(f, "Returning if let Fork::Parent(child_pid)|").unwrap();
            writeln!(f, "We are now in the parent process of the fork|").unwrap();
            writeln!(f, "parent process pid: {} |", std::process::id()).unwrap();
            return Ok(Some(child_pid));
        }

        writeln!(f, "We are now in the child process of the fork|").unwrap();
        writeln!(f, "child process pid: {} |", std::process::id()).unwrap();

        if self.daemonize {
            writeln!(f, "Daemonizing process pid: {} |", std::process::id()).unwrap();
            match unsafe { fork()? } {
                Fork::Parent(_) => {
                    writeln!(f, "Fork::Parent Daemonize, immediately exiting|").unwrap();
                    std::process::exit(0);
                }
                Fork::Child => {
                    writeln!(f, "Fork::Child Daemonize, cur_pid: {}|", std::process::id()).unwrap();
                    writeln!(f, "the current process' parent BEFORE libc::setside: {}|", std::os::unix::process::parent_id()).unwrap();
                    // put the child in a new session to reparent it to init and fully daemonize it
                    unsafe { 
                        let pid = libc::setsid();
                        writeln!(f, "setsid returned pid: {}|", pid).unwrap();
                        pid
                    };
                    writeln!(f, "current process id after libc::setside: {}|", std::process::id()).unwrap();
                    let s = System::new_all();

                    writeln!(f, "the current process' parent AFTER libc::setside: {}|", std::os::unix::process::parent_id()).unwrap();
    
                    writeln!(f, "printing processes after libc::setside|").unwrap();
                    for (pid, process) in s.processes() {
                        writeln!(f, "process: {} {} {} {:?}|", pid, process.exe().to_string_lossy(), process.name(), process.status()).unwrap();
                    }
                }
            }
        }

        if let Some(fd) = stdin.as_fd() {
            unsafe { libc::dup2(fd, libc::STDIN_FILENO) };
        }

        if let Some(fd) = stdout.as_fd() {
            unsafe { libc::dup2(fd, libc::STDOUT_FILENO) };
        }

        if let Some(fd) = stderr.as_fd() {
            unsafe { libc::dup2(fd, libc::STDERR_FILENO) };
        }

        writeln!(f, "pid of process that is about to call spawn: {}", std::process::id()).unwrap();

        spawn();

        std::process::exit(1);
    }
}

pub struct Child {
    pub pid: Option<libc::pid_t>,
}

impl Child {
    pub fn wait(self) -> anyhow::Result<WaitStatus> {
        let pid = match self.pid {
            Some(pid) => Pid::from_raw(pid),
            None => return Ok(WaitStatus::Exited(Pid::from_raw(0), 0)),
        };

        Ok(nix::sys::wait::waitpid(Some(pid), None)?)
    }
}
