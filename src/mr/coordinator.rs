#![allow(unused)]

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use anyhow::Result;
use async_stream;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, Notify};
use tokio::time;
use tonic::{transport::Server, Request, Response, Status};

use crate::util::condvar::Condvar;
use crate::util::net as netutil;
use mr_types::{
    coordinator_server::{Coordinator, CoordinatorServer},
    AskForJobReply, Empty, Job, JobStatus, JobType, ReportJobStatusRequest,
};

pub mod mr_types {
    include!("../../proto/mr.rs");
}

static JOB_TIMEOUT: time::Duration = time::Duration::from_secs(10);

#[derive(Debug)]
enum WorkloadPhase {
    Mapping,
    Reducing,
    Complete,
}

impl Default for WorkloadPhase {
    fn default() -> Self {
        WorkloadPhase::Mapping
    }
}

#[derive(Debug)]
struct RunningJob {
    job: Job,
    start_time: time::Instant,
}

#[derive(Debug, Default)]
pub struct MRCoordinator {
    num_map_jobs: u32,
    num_reduce_jobs: u32,

    waiting_map_jobs: Vec<Job>,
    waiting_reduce_jobs: Vec<Job>,

    running_map_jobs: HashMap<u32, RunningJob>,
    running_reduce_jobs: HashMap<u32, RunningJob>,

    complete_map_jobs: Vec<Job>,
    complete_reduce_jobs: Vec<Job>,

    workload_phase: WorkloadPhase,
}

#[derive(Debug)]
pub struct MutexMRCoordinator {
    locked_coordinator: Arc<Mutex<MRCoordinator>>,
    cond: Condvar,
}

impl MutexMRCoordinator {
    pub fn new(files: Vec<String>, num_reducer: u32) -> Self {
        MutexMRCoordinator {
            locked_coordinator: Arc::new(Mutex::new(MRCoordinator {
                num_map_jobs: files.len() as u32,
                num_reduce_jobs: num_reducer,
                waiting_map_jobs: Self::gen_map_jobs(files),
                ..Default::default()
            })),
            cond: Condvar::new(),
        }
    }

    fn gen_map_jobs(files: Vec<String>) -> Vec<Job> {
        let mut ret = vec![];
        for (i, f) in files.iter().enumerate() {
            ret.push(Job {
                id: i as u32,
                job_type: JobType::Map as i32,
                inp_file: f.to_string(),
                oup_file: String::new(),
            });
        }
        ret
    }

    fn gen_reduce_jobs(num_reducer: u32) -> Vec<Job> {
        let mut ret = vec![];
        for i in 0..num_reducer {
            ret.push(Job {
                id: i,
                job_type: JobType::Reduce as i32,
                inp_file: format!("mr-inp-{}", i),
                oup_file: format!("mr-out-{}", i),
            })
        }
        ret
    }

    async fn reset_timeout_jobs(crdnt_cpy: Arc<Mutex<MRCoordinator>>) {
        println!("start the running jobs checker...");
        loop {
            time::sleep(time::Duration::from_secs(2)).await;
            let mut coordinator = (*crdnt_cpy).lock().await;
            match &coordinator.workload_phase {
                WorkloadPhase::Mapping => {
                    coordinator.running_map_jobs.retain(|job_id, job| {
                        if job.start_time + JOB_TIMEOUT > time::Instant::now() {
                            return true;
                        }
                        println!("running map job {} timeout", job_id);
                        false
                    });
                }
                WorkloadPhase::Reducing => {
                    coordinator.running_reduce_jobs.retain(|job_id, job| {
                        if job.start_time + JOB_TIMEOUT > time::Instant::now() {
                            return true;
                        }
                        println!("running reduce job {} timeout", job_id);
                        false
                    });
                }
                WorkloadPhase::Complete => return,
            }
            drop(coordinator);
        }
    }

    pub async fn run(files: Vec<String>, num_reducer: u32) -> Result<()> {
        let coordinator = Self::new(files, num_reducer);
        let coordinator_addr: SocketAddr = netutil::COORDINATOR_ADDR
            .parse()
            .expect("failed to parse the coordinator's address");
        println!("MAPPING...");
        let coordinator_cpy = coordinator.locked_coordinator.clone();
        tokio::spawn(async move {
            Self::reset_timeout_jobs(coordinator_cpy).await;
        });
        Server::builder()
            .add_service(CoordinatorServer::new(coordinator))
            .serve(coordinator_addr)
            .await?;
        Ok(())
    }
}

#[tonic::async_trait]
impl Coordinator for MutexMRCoordinator {
    async fn ask_for_job(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<AskForJobReply>, Status> {
        let mut reply: AskForJobReply;

        println!("worker is asking for job...");

        loop {
            let mut coordinator = self.locked_coordinator.lock().await;
            reply = match &coordinator.workload_phase {
                WorkloadPhase::Mapping => {
                    if coordinator.waiting_map_jobs.len() == 0 {
                        self.cond.wait(coordinator, &self.locked_coordinator).await;
                        continue;
                    }

                    let assigned_job = coordinator.waiting_map_jobs.remove(0);
                    coordinator.running_map_jobs.insert(
                        assigned_job.id,
                        RunningJob {
                            job: assigned_job.clone(),
                            start_time: time::Instant::now(),
                        },
                    );
                    AskForJobReply {
                        assigned_job: Some(assigned_job),
                        num_reducer: coordinator.num_reduce_jobs,
                    }
                }

                WorkloadPhase::Reducing => {
                    if coordinator.waiting_reduce_jobs.len() == 0 {
                        self.cond.wait(coordinator, &self.locked_coordinator).await;
                        continue;
                    }
                    let assigned_job = coordinator.waiting_reduce_jobs.remove(0);
                    coordinator.running_reduce_jobs.insert(
                        assigned_job.id,
                        RunningJob {
                            job: assigned_job.clone(),
                            start_time: time::Instant::now(),
                        },
                    );
                    AskForJobReply {
                        assigned_job: Some(assigned_job),
                        num_reducer: coordinator.num_reduce_jobs,
                    }
                }

                default =>
                // workload is complete
                {
                    println!("workload complete, would not assign any job to the worker");
                    AskForJobReply::default()
                }
            };

            break;
        }
        Ok(Response::new(reply))
    }

    async fn report_job_status(
        &self,
        request: Request<ReportJobStatusRequest>,
    ) -> Result<Response<Empty>, Status> {
        let mut coordinator = self.locked_coordinator.lock().await;
        let req_inner = request.into_inner();
        let status = JobStatus::from_i32(req_inner.status).unwrap();
        let job_type = JobType::from_i32(req_inner.job_type).unwrap();
        let id = req_inner.job_id;
        println!("received report for job {}", req_inner.job_id);
        match job_type {
            JobType::Map => {
                if status == JobStatus::JobComplete {
                    println!("{:?} job({}) has completed", job_type, id);
                    let job_info = coordinator.running_map_jobs.remove(&id).unwrap();
                    coordinator.complete_map_jobs.push(job_info.job);
                    if coordinator.complete_map_jobs.len() == coordinator.num_map_jobs as usize {
                        println!("REDUCEING...");
                        // entering the reducing phase
                        coordinator.waiting_reduce_jobs =
                            Self::gen_reduce_jobs(coordinator.num_reduce_jobs);
                        coordinator.workload_phase = WorkloadPhase::Reducing;
                        self.cond.notify_waiters(coordinator);
                    }
                } else {
                    // job failed
                    println!("{:?} job({}) has failed", job_type, id);
                    let job_info = coordinator.running_map_jobs.remove(&id).unwrap();
                    coordinator.waiting_map_jobs.push(job_info.job);
                    self.cond.notify_waiters(coordinator);
                }
            }

            JobType::Reduce => {
                if status == JobStatus::JobComplete {
                    println!("{:?} job({}) has completed", job_type, id);
                    let job_info = coordinator.running_reduce_jobs.remove(&id).unwrap();
                    coordinator.complete_reduce_jobs.push(job_info.job);
                    if coordinator.complete_reduce_jobs.len()
                        == coordinator.num_reduce_jobs as usize
                    {
                        // workload has completed
                        println!("COMPLETE");
                        coordinator.workload_phase = WorkloadPhase::Complete;
                        self.cond.notify_waiters(coordinator);
                    }
                } else {
                    // job failed
                    println!("{:?} job({}) has failed", job_type, id);
                    let job_info = coordinator.running_reduce_jobs.remove(&id).unwrap();
                    coordinator.waiting_reduce_jobs.push(job_info.job);
                    self.cond.notify_waiters(coordinator);
                }
            }
        };

        Ok(Response::new(Empty {}))
    }
}
