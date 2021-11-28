use fnv::FnvHasher;
use libloading::{Library, Symbol};
use mr_types::{
    coordinator_client::CoordinatorClient, Empty, Job, JobStatus, JobType, ReportJobStatusRequest,
};
use nix::fcntl::{flock, FlockArg};
use serde::{Deserialize, Serialize};

use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::io::AsRawFd;

use crate::util::net as netutil;
pub mod mr_types {
    include!("../../proto/mr.rs");
}

#[derive(Deserialize, Serialize, Debug)]
pub struct KeyValue<T, U> {
    pub key: T,
    pub val: U,
}

impl<T, U> KeyValue<T, U> {
    pub fn new(key: T, val: U) -> Self {
        KeyValue { key, val }
    }
}

type MapFunc = fn(String, String) -> Vec<KeyValue<String, u8>>;
type ReduceFunc = fn(&str, Vec<u8>) -> usize;

pub async fn start_worker(libpath: String) -> Result<(), Box<dyn std::error::Error>> {
    let mapfunc: Symbol<MapFunc>;
    let reducefunc: Symbol<ReduceFunc>;

    unsafe {
        let lib = Library::new(libpath).expect("failed to load library");
        mapfunc = lib.get(b"map").unwrap();
        reducefunc = lib.get(b"reduce").unwrap();

        let mut client = CoordinatorClient::connect(netutil::COORDINATOR_ADDR).await?;

        loop {
            let response = client.ask_for_job(Empty {}).await?;
            let reply = response.into_inner();
            if let None = reply.assigned_job {
                println!("there is no pending job, stop the worker...");
                return Ok(());
            }
            let job = reply.assigned_job.unwrap();
            let num_reducer = reply.num_reducer;

            match JobType::from_i32(job.job_type) {
                Some(JobType::Map) => {
                    handle_map_job(&mut client, job, num_reducer, &mapfunc).await?;
                }

                Some(JobType::Reduce) => {
                    handle_reduce_job(&mut client, job, &reducefunc).await?;
                }

                None => {
                    panic!("failed to convert job type from i32");
                }
            };
        }
    }
}

async fn handle_map_job(
    client: &mut CoordinatorClient<tonic::transport::Channel>,
    job: Job,
    num_reducer: u32,
    mapfunc: &libloading::Symbol<'_, MapFunc>,
) -> Result<(), Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(&job.inp_file).expect("");
    let intermediate = mapfunc(job.inp_file.clone(), content);
    for kv in intermediate {
        let reducer = ihash(&kv.key) % (num_reducer as u64);
        let file_name = format!("mr-inp-{}", reducer);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(file_name)
            .expect("");
        let fd = file.as_raw_fd();
        flock(fd, FlockArg::LockExclusive).expect("TODO");
        serde_json::to_writer(file, &kv)?;
        flock(fd, FlockArg::Unlock).expect("TODO");
    }

    let rjs_request = tonic::Request::new(ReportJobStatusRequest {
        job_id: job.id,
        job_type: JobType::Map as i32,
        status: JobStatus::JobComplete as i32,
    });

    let _ = client.report_job_status(rjs_request).await?;
    Ok(())
}

async fn handle_reduce_job(
    client: &mut CoordinatorClient<tonic::transport::Channel>,
    job: Job,
    reducefunc: &libloading::Symbol<'_, ReduceFunc>,
) -> Result<(), Box<dyn std::error::Error>> {
    // read intermediate data
    let file = std::fs::File::open(&job.inp_file)?;
    let mut intermediate = vec![];
    for line in BufReader::new(file).lines() {
        if let Ok(json_str) = line {
            let kv: KeyValue<String, u8> = serde_json::from_str(&json_str).expect("TODO");
            intermediate.push(kv);
        }
    }
    // sort lexicographically
    intermediate.sort_by(|a, b| a.key.cmp(&b.key));

    // collapse entries with same key
    collapse(&job.oup_file, intermediate, reducefunc)?;

    // report the job status
    let rjs_request = tonic::Request::new(ReportJobStatusRequest {
        job_id: job.id,
        job_type: JobType::Reduce as i32,
        status: JobStatus::JobComplete as i32,
    });

    let _ = client.report_job_status(rjs_request).await?;
    Ok(())
}

fn ihash(key: &str) -> u64 {
    let mut fnv_hahser = FnvHasher::default();
    key.hash(&mut fnv_hahser);
    fnv_hahser.finish() & u64::from_str_radix("7fffffff", 16).unwrap()
}

fn collapse(
    oup_file_name: &str,
    intermediate: Vec<KeyValue<String, u8>>,
    reducefunc: &libloading::Symbol<'_, ReduceFunc>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut of = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(oup_file_name)
        .expect("failed to open the output file");
    let mut i = 0;
    let len = intermediate.len();
    while i < len {
        let mut count: usize = 0;
        for j in i..len {
            if intermediate[i].key != intermediate[j].key {
                break;
            }
            count += 1;
        }

        let mut vals = vec![];
        for _ in 0..count {
            vals.push(intermediate[i].val);
        }

        let result = reducefunc(&intermediate[i].key, vals);
        of.write_all(format!("{} {}\n", &intermediate[i].key, result).as_bytes())
            .expect("failed to write to file");
        i += count;
    }
    Ok(())
}
