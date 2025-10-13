fn main() {
    // Create a directory for the proto files
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let proto_dir = std::path::PathBuf::from(&manifest_dir).join("proto");
    std::fs::create_dir_all(&proto_dir).expect("Failed to create proto directory");

    // Define proto files
    let proto_files = vec![
        ("user_event.proto", include_str!("proto/user_event.proto")),
        ("user.proto", include_str!("proto/user.proto")),
        ("post.proto", include_str!("proto/post.proto")),
        (
            "user_post_relation.proto",
            include_str!("proto/user_post_relation.proto"),
        ),
    ];

    // Write proto files and collect their paths
    let mut proto_paths = Vec::new();
    for (filename, content) in proto_files {
        let proto_file = proto_dir.join(filename);
        std::fs::write(&proto_file, content)
            .unwrap_or_else(|_| panic!("Failed to write {filename}"));
        proto_paths.push(proto_file);
    }

    // Compile all proto files using cargo_out_dir
    protobuf_codegen::Codegen::new()
        .protoc()
        .includes([&proto_dir])
        .inputs(&proto_paths)
        .cargo_out_dir("protos")
        .run_from_script();
}
