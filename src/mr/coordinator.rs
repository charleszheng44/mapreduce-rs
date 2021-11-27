#![allow(unused)]

use async_stream;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, Notify};
use tonic::{transport::Server, Request, Response, Status};

use mr_types::{
    coordinator_server::{Coordinator, CoordinatorServer},
    AskForJobReply, Empty, Job, JobStatus, JobType, ReportJobStatusRequest,
};

pub mod mr_types {
    include!("../../proto/mr.rs");
}

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

#[derive(Debug, Default)]
pub struct MRCoordinator {
    num_map_jobs: u32,
    num_reduce_jobs: u32,

    waiting_map_jobs: Vec<Job>,
    waiting_reduce_jobs: Vec<Job>,

    running_map_jobs: HashMap<u32, Job>,
    running_reduce_jobs: HashMap<u32, Job>,

    complete_map_jobs: Vec<Job>,
    complete_reduce_jobs: Vec<Job>,

    workload_phase: WorkloadPhase,
}

#[derive(Debug)]
pub struct MutexMRCoordinator {
    locked_coordinator: Mutex<MRCoordinator>,
    notifier: Notify,
}

impl MutexMRCoordinator {
    pub fn new(files: Vec<String>, num_reducer: u32) -> Self {
        MutexMRCoordinator {
            locked_coordinator: Mutex::new(MRCoordinator {
                num_map_jobs: files.len() as u32,
                num_reduce_jobs: num_reducer,
                waiting_map_jobs: Self::gen_map_jobs(files),
                ..Default::default()
            }),
            notifier: Notify::new(),
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
                inp_file: format!("mr-in-{}", i),
                oup_file: format!("mr-out-{}", i),
            })
        }
        ret
    }

    async fn run(
        addr: SocketAddr,
        files: Vec<String>,
        num_reducer: u32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let coordinator = Self::new(files, num_reducer);
        let coordinator_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
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
        let mut coordinator = self.locked_coordinator.lock().await;
        let mut reply: AskForJobReply;

        loop {
            reply = match &coordinator.workload_phase {
                WorkloadPhase::Mapping => {
                    if coordinator.waiting_map_jobs.len() == 0 {
                        self.notifier.notified().await;
                        continue;
                    }

                    let assigned_job = coordinator.waiting_map_jobs.remove(0);
                    coordinator
                        .running_map_jobs
                        .insert(assigned_job.id, assigned_job.clone());
                    AskForJobReply {
                        assigned_job: Some(assigned_job),
                        num_reducer: coordinator.num_reduce_jobs,
                    }
                    // TODO what if the job has been timeout?
                }

                WorkloadPhase::Reducing => {
                    if coordinator.waiting_reduce_jobs.len() == 0 {
                        self.notifier.notified().await;
                        continue;
                    }
                    let assigned_job = coordinator.waiting_reduce_jobs.remove(0);
                    coordinator
                        .running_reduce_jobs
                        .insert(assigned_job.id, assigned_job.clone());
                    AskForJobReply {
                        assigned_job: Some(assigned_job),
                        num_reducer: coordinator.num_reduce_jobs,
                    }
                    // TODO what if the job has been timeout?
                }

                default =>
                // workload is complete
                {
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

        match job_type {
            JobType::Map => {
                if status == JobStatus::JobComplete {
                    println!("{:?} job({}) has completed", job_type, id);
                    let job = coordinator.running_map_jobs.remove(&id).unwrap();
                    coordinator.complete_map_jobs.push(job);
                    if coordinator.complete_map_jobs.len() == coordinator.num_map_jobs as usize {
                        // entering the reducing phase
                        coordinator.workload_phase = WorkloadPhase::Reducing;
                        self.notifier.notify_waiters();
                    }
                } else {
                    // job failed
                    println!("{:?} job({}) has failed", job_type, id);
                    let job = coordinator.running_map_jobs.remove(&id).unwrap();
                    coordinator.waiting_map_jobs.push(job);
                    self.notifier.notify_waiters();
                }
            }

            JobType::Reduce => {
                if status == JobStatus::JobComplete {
                    println!("{:?} job({}) has completed", job_type, id);
                    let job = coordinator.running_reduce_jobs.remove(&id).unwrap();
                    coordinator.complete_reduce_jobs.push(job);
                    if coordinator.complete_reduce_jobs.len()
                        == coordinator.num_reduce_jobs as usize
                    {
                        // workload has completed
                        coordinator.workload_phase = WorkloadPhase::Complete;
                        self.notifier.notify_waiters();
                    }
                } else {
                    // job failed
                    println!("{:?} job({}) has failed", job_type, id);
                    let job = coordinator.running_reduce_jobs.remove(&id).unwrap();
                    coordinator.waiting_reduce_jobs.push(job);
                    self.notifier.notify_waiters();
                }
            }
        };

        Ok(Response::new(Empty {}))
    }
}
