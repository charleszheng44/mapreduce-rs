// import from the external crates
use fnv::FnvHasher;
use libloading::{Library, Symbol};
use mr_types::{
    coordinator_client::CoordinatorClient, Empty, Job, JobStatus, JobType, ReportJobStatusRequest,
};
use nix::fcntl::{flock, FlockArg};
use serde::{Deserialize, Serialize};
// import from the std crates
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::io::AsRawFd;
// import from the current crate
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

        let dest = format!("http://{}", netutil::COORDINATOR_ADDR);
        let mut client = CoordinatorClient::connect(dest).await?;

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
                    let id = job.id;
                    println!("handling map job({})...", id);
                    handle_map_job(&mut client, job, num_reducer, &mapfunc).await?;
                    println!("successfully handled the map job({})...", id);
                }

                Some(JobType::Reduce) => {
                    println!("handling reduce job({})...", job.id);
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
    let content = std::fs::read_to_string(&job.inp_file)
        .unwrap_or_else(|_| panic!("failed to read from file {}", job.inp_file));
    let intermediate = mapfunc(job.inp_file.clone(), content);

    // write the intermediate results to the buffer.
    let mut oup_buf = vec![String::new(); num_reducer as usize];
    for kv in intermediate {
        let reducer = ihash(&kv.key) % (num_reducer as u64);
        let js_string = serde_json::to_string(&kv)?;
        oup_buf[reducer as usize].push_str(&js_string);
        oup_buf[reducer as usize].push_str("\n");
    }

    // flush the buffer to the file.
    for (i, oup) in oup_buf.iter().enumerate() {
        let file_name = format!("mr-inp-{}", i);
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(file_name.clone())
            .unwrap_or_else(|_| panic!("failed to open the file {}", file_name));
        let fd = file.as_raw_fd();
        flock(fd, FlockArg::LockExclusive)
            .unwrap_or_else(|_| panic!("failed to lock the file {}", file_name));
        file.write(oup.as_bytes())?;
        drop(file);
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
            let kv: KeyValue<String, u8> = serde_json::from_str(&json_str)
                .unwrap_or_else(|_| panic!("failed to parse the json string({})", json_str));
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
