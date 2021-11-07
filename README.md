# Sample MapReduce

This repository contains a sample mapreduce implementation in Rust for [mit 6.824](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)

## Sample Sequential MapReduce

### How to Run

1. Generate the dynamic library
```bash
$ cd mrapps-wc
$ cargo build
$ cd ..
```

2. Generate the `mrsequential` executable
```bash
$ cargo build
```

3. Run the executable
```bas
./target/debug/mrsequential target/debug/libmrapps_wc.dylib testfiles/*
```
If everything goes well, the mr-out-0 file will be generated.
