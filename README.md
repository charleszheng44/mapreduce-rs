# Sample MapReduce

This repository contains a sample mapreduce implementation in Rust for [mit 6.824](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)

## Sample Sequential MapReduce

### How to Run

1. Generate the dynamic library
```bash
$ cargo build
```

2. Generate the `mrsequential` executable
```bash
$ cargo build
```

3. Run the mapreduce sequentially
```bash
$PROJECT_DIR/target/debug/mrsequential $PROJECT_DIR/target/debug/libmrapps_wc.dylib $PROJECT_DIR/testfiles/*
```
If everything goes well, the mr-out-0 file will be generated.

4. Run the distributed version   
Start the coordinator in one shell:
```bash
$PROJECT_DIR/target/debug/mrcoordinator $PROJECT_DIR/testfiles/pg-*
```
Start the mrworkers in another shell:
```bash
# start 5 mrworkers process
$PROJECT_DIR/scripts/run_workers.sh 5
```
