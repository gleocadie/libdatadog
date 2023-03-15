use std::{
    env,
    io::{Read, Write},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    thread::{self, JoinHandle},
    time::Duration,
};

use std::str;

use serde::{Deserialize, Serialize};

use nix::libc::STDOUT_FILENO;

use anyhow::Result;

pub struct ServerlessLogsAgent {}

#[derive(Serialize, Deserialize, Debug)]
struct LogsMessage<'a> {
    #[serde(rename(serialize = "ddsource"))]
    dd_source: &'a str,
    #[serde(rename(serialize = "ddtags"))]
    dd_tags: &'a str,
    hostname: &'a str,
    message: &'a str,
    service: &'a str,
}

impl ServerlessLogsAgent {
    pub fn run(&self) -> Result<JoinHandle<()>> {
        // Err(anyhow::format_err!("Error!")
        let (read_end, write_end) = nix::unistd::pipe()?;
        let original_stdout = nix::unistd::dup(STDOUT_FILENO)?;
        nix::unistd::dup2(write_end, STDOUT_FILENO)?;
        nix::unistd::close(write_end)?;
        let mut read_end = unsafe { FileDesc::from_raw_fd(read_end) };
        let mut original_stdout = unsafe { FileDesc::from_raw_fd(original_stdout) };

        let client = reqwest::blocking::Client::new();

        let join = thread::spawn(move || loop {
            loop {
                let mut buf = [0; 1000];
                let read = match read_end.read(&mut buf) {
                    Ok(s) => s,
                    Err(er) => {
                        eprintln!("{}", er);
                        break;
                    }
                };

                let message = LogsMessage {
                    dd_source: "nginx",
                    dd_tags: "ivan:poc",
                    service: "ivanpoc",
                    hostname: "ivanpoc",
                    message: str::from_utf8(&buf[0..read])
                        .expect("error converting log line to str"),
                };

                let json_message =
                    serde_json::to_string(&message).expect("Error converting struct to log");

                // eprintln!("{}", json_message);

                let dd_api_key = env::var("DD_API_KEY").expect("Please set DD_API_KEY");

                let request = client
                    .post("https://http-intake.logs.datadoghq.com/api/v2/logs")
                    .header("Accept", "application/json")
                    .header("Content-Type", "application/json")
                    .header("DD-API-KEY", dd_api_key)
                    .body(json_message);

                let response = request.send();

                if let Err(err) = original_stdout.write_all(&buf[0..read]) {
                    eprintln!("{}", err);
                    break;
                };
                thread::sleep(Duration::from_micros(10));
            }
        });
        Ok(join)
    }
}

trait Agent: Sized {
    fn run(&self) -> Result<JoinHandle<()>>;
}

struct FileDesc(OwnedFd);

impl FromRawFd for FileDesc {
    unsafe fn from_raw_fd(fd: std::os::fd::RawFd) -> Self {
        Self(OwnedFd::from_raw_fd(fd))
    }
}

impl Write for FileDesc {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        Ok(nix::unistd::write(self.0.as_raw_fd(), buf)?)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Read for FileDesc {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(nix::unistd::read(self.0.as_raw_fd(), buf)?)
    }
}
