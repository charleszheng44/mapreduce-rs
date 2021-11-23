#![allow(unused)]

use std::collections::HashMap;

use tonic::{Request, Response, Status};

use mr_types::{
    coordinator_server::Coordinator, AskForJobReply, Empty, Job, JobStatus, JobType,
    ReportJobStatusRequest,
};
use tokio::sync::{Mutex, Notify};

pub mod mr_types {
    include!("../../proto/mr.rs");
}

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

#[derive(Default)]
pub struct MRCoordinator {
    lock: Mutex<()>,
    notifier: Notify,

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

impl<'a> MRCoordinator {
    pub fn new(files: Vec<String>, num_reducer: u32) -> Self {
        MRCoordinator {
            num_map_jobs: files.len() as u32,
            num_reduce_jobs: num_reducer,
            waiting_map_jobs: Self::gen_map_jobs(files),
            ..Default::default()
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

    // TODO NOT IMPLEMENT YET
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        todo!("NOT IMPLEMENT YET")
    }
}

#[tonic::async_trait]
impl Coordinator for MRCoordinator {
    async fn ask_for_job(
        &mut self,
        _request: Request<Empty>,
    ) -> Result<Response<AskForJobReply>, Status> {
        let _ = self.lock.lock().await;
        let mut reply: AskForJobReply;

        loop {
            reply = match &self.workload_phase {
                WorkloadPhase::Mapping => {
                    if self.waiting_map_jobs.len() == 0 {
                        self.notifier.notified().await;
                        continue;
                    }

                    let assigned_job = self.waiting_map_jobs.remove(0);
                    self.running_map_jobs
                        .insert(assigned_job.id, assigned_job.clone());
                    AskForJobReply {
                        assigned_job: Some(assigned_job),
                        num_reducer: self.num_reduce_jobs,
                    }
                    // TODO what if the job has been timeout?
                }

                WorkloadPhase::Reducing => {
                    if self.waiting_reduce_jobs.len() == 0 {
                        self.notifier.notified().await;
                        continue;
                    }
                    let assigned_job = self.waiting_reduce_jobs.remove(0);
                    self.running_reduce_jobs
                        .insert(assigned_job.id, assigned_job.clone());
                    AskForJobReply {
                        assigned_job: Some(assigned_job),
                        num_reducer: self.num_reduce_jobs,
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
        &mut self,
        request: Request<ReportJobStatusRequest>,
    ) -> Result<Response<Empty>, Status> {
        let _ = self.lock.lock().await;
        let req_inner = request.into_inner();
        let status = JobStatus::from_i32(req_inner.status).unwrap();
        let job_type = JobType::from_i32(req_inner.job_type).unwrap();
        let id = req_inner.job_id;

        match job_type {
            JobType::Map => {
                if status == JobStatus::JobComplete {
                    println!("{:?} job({}) has completed", job_type, id);
                    let job = self.running_map_jobs.remove(&id).unwrap();
                    self.complete_map_jobs.push(job);
                    if self.complete_map_jobs.len() == self.num_map_jobs as usize {
                        // entering the reducing phase
                        self.workload_phase = WorkloadPhase::Reducing;
                        self.notifier.notify_waiters();
                    }
                } else {
                    // job failed
                    println!("{:?} job({}) has failed", job_type, id);
                    let job = self.running_map_jobs.remove(&id).unwrap();
                    self.waiting_map_jobs.push(job);
                    self.notifier.notify_waiters();
                }
            }

            JobType::Reduce => {
                if status == JobStatus::JobComplete {
                    println!("{:?} job({}) has completed", job_type, id);
                    let job = self.running_reduce_jobs.remove(&id).unwrap();
                    self.complete_reduce_jobs.push(job);
                    if self.complete_reduce_jobs.len() == self.num_reduce_jobs as usize {
                        // workload has completed
                        self.workload_phase = WorkloadPhase::Complete;
                        self.notifier.notify_waiters();
                    }
                } else {
                    // job failed
                    println!("{:?} job({}) has failed", job_type, id);
                    let job = self.running_reduce_jobs.remove(&id).unwrap();
                    self.waiting_reduce_jobs.push(job);
                    self.notifier.notify_waiters();
                }
            }
        };

        Ok(Response::new(Empty {}))
    }
}
