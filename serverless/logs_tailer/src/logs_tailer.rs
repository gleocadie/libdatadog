use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use std::io::{self, BufRead};

#[no_mangle]
pub extern "C" fn process_stdin() {
    println!("Starting logs tailer");

    println!("Ready. Sending signal to parent.");

    signal::kill(Pid::parent(), Signal::SIGUSR1).unwrap();
    signal::kill(Pid::parent(), Signal::SIGUSR1).unwrap();
    signal::kill(Pid::parent(), Signal::SIGUSR1).unwrap();
    signal::kill(Pid::parent(), Signal::SIGUSR1).unwrap();

    for line in io::stdin().lock().lines() {
        println!("{}", line.unwrap());
    }
}

pub fn main() {
    process_stdin()
}
