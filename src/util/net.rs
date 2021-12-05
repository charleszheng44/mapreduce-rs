#![allow(unused)]
use std::process::Command;

// TODO tonic does not support uds well. a workable example is
// https://github.com/hyperium/tonic/blob/c62f382e3c6e9c0641decfafb2b8396fe52b6314/examples/src/uds/server.rs#L49
pub const COORDINATOR_ADDR: &str = "127.0.0.1:8080";

/// coordinator_sock cooks up a unique-ish UNIX-domain socket name
/// in /var/tmp, for the coordinator.
pub fn coordinator_sock() -> String {
    let mut sock_file = "/var/tmp/824-mr-".to_owned();
    let output = if cfg!(target_os = "windows") {
        panic!("can not generate sock file on windows");
    } else {
        Command::new("id")
            .arg("-u")
            .output()
            .expect("failed to execute process")
    };
    let uid = String::from_utf8(output.stdout).expect("failed to convert command result to string");
    sock_file.push_str(uid.as_str());
    sock_file
}
