fn main() {
    let proto_files = &["proto/mr.proto"];
    let dirs = &["."];
    tonic_build::configure()
        .out_dir("proto")
        .build_client(true)
        .build_server(true)
        .compile(proto_files, dirs)
        .unwrap_or_else(|e| panic!("protobuf compilation failed: {}", e));

    // recompile protobufs only if any of the proto files changes.
    for file in proto_files {
        println!("cargo:rerun-if-changed={}", file);
    }
}
