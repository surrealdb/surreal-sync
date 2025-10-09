fn main() {
    // Create a directory for the proto file
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let proto_dir = std::path::PathBuf::from(&manifest_dir).join("proto");
    std::fs::create_dir_all(&proto_dir).expect("Failed to create proto directory");

    let proto_file = proto_dir.join("user_event.proto");

    let proto_content = r#"
syntax = "proto3";

package example;

message UserEvent {
    string user_id = 1;
    string event_type = 2;
    int64 timestamp = 3;
    string data = 4;
}
"#;

    std::fs::write(&proto_file, proto_content).expect("Failed to write proto file");

    // Compile the proto file using cargo_out_dir
    protobuf_codegen::Codegen::new()
        .protoc()
        .includes(&[&proto_dir])
        .input(&proto_file)
        .cargo_out_dir("protos")
        .run_from_script();
}
