use std::{process::Stdio, thread};

use logs::logs::ServerlessLogsAgent;
use std::process::Command;

fn main() {
    let logs_agent = ServerlessLogsAgent {};
    logs_agent.run();

    println!("This is a test");

    thread::sleep_ms(3000);

    Command::new("sh")
        .arg("-c")
        .arg("echo hello")
        .stdout(Stdio::inherit())
        .output()
        .expect("failed to execute process");

    println!("This is a test");
}
