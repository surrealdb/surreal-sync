//! Loadtest generate command handler.

use anyhow::Context;
use loadtest_distributed::{
    build_cluster_config,
    generator::{ConfigGenerator, DockerComposeGenerator, KubernetesGenerator},
    GenerateArgs, Platform, SourceType,
};
use loadtest_populate_kafka::generate_proto_for_table;

/// Run the loadtest generate command.
pub async fn run_loadtest_generate(args: GenerateArgs) -> anyhow::Result<()> {
    tracing::info!("Generating load test configuration...");
    tracing::info!("Platform: {:?}", args.platform);
    tracing::info!("Source: {:?}", args.source);
    tracing::info!("Preset: {:?}", args.preset);

    // Convert CLI enums to internal types
    let preset_size = args.preset.into();
    let source_type = args.source.into();
    let platforms = args.platform.to_platforms();

    // Get tables from schema file
    let schema_content = std::fs::read_to_string(&args.schema)
        .with_context(|| format!("Failed to read schema file: {:?}", args.schema))?;
    let schema: sync_core::GeneratorSchema =
        serde_yaml::from_str(&schema_content).with_context(|| "Failed to parse schema YAML")?;
    let tables: Vec<String> = schema.tables.iter().map(|t| t.name.clone()).collect();

    // Build cluster configuration
    let mut config = build_cluster_config(
        preset_size,
        source_type,
        platforms[0], // Use first platform for config
        args.num_containers,
        args.cpu_limit.clone(),
        args.memory_limit.clone(),
        if args.tmpfs {
            args.tmpfs_size.clone()
        } else {
            None
        },
        args.row_count,
        args.batch_size,
        Some(args.schema.clone()),
        &tables,
        args.dry_run,
        args.surrealdb_image.clone(),
    )?;

    // Set schema content for Kubernetes ConfigMap embedding
    if platforms.contains(&Platform::Kubernetes) {
        config.schema_content = Some(schema_content.clone());

        // For Kafka source, generate proto files and embed in ConfigMap
        if source_type == SourceType::Kafka {
            let mut proto_contents = std::collections::HashMap::new();
            for table in &schema.tables {
                let proto_content = generate_proto_for_table(table, "loadtest");
                proto_contents.insert(table.name.clone(), proto_content);
            }
            config.proto_contents = Some(proto_contents);
        }
    }

    // Output ClusterConfig as JSON for CI tooling (single line, easy to parse)
    // This allows run_ci.py to get the effective row_count used by the generator
    println!("{}", serde_json::to_string(&config)?);

    // Create output directory
    let output_dir = &args.output_dir;
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("Failed to create output directory: {output_dir:?}"))?;

    // Generate configurations
    for platform in &platforms {
        match platform {
            Platform::DockerCompose => {
                let generator = DockerComposeGenerator;
                let content = generator.generate(&config)?;
                let path = output_dir.join(generator.filename());
                std::fs::write(&path, &content)
                    .with_context(|| format!("Failed to write {}", path.display()))?;
                tracing::info!("Generated: {}", path.display());
            }
            Platform::Kubernetes => {
                let generator = KubernetesGenerator;

                // Option 1: Single file
                let content = generator.generate(&config)?;
                let path = output_dir.join(generator.filename());
                std::fs::write(&path, &content)
                    .with_context(|| format!("Failed to write {}", path.display()))?;
                tracing::info!("Generated: {}", path.display());

                // Option 2: Multiple files in subdirectory
                let k8s_dir = output_dir.join("kubernetes");
                std::fs::create_dir_all(&k8s_dir)?;
                let files = generator.generate_to_files(&config)?;
                for (filename, content) in files {
                    let path = k8s_dir.join(&filename);
                    std::fs::write(&path, &content)
                        .with_context(|| format!("Failed to write {}", path.display()))?;
                    tracing::info!("Generated: {}", path.display());
                }
            }
        }
    }

    // Copy schema file
    let schema_dest = output_dir.join("config").join("schema.yaml");
    std::fs::create_dir_all(schema_dest.parent().unwrap())?;
    std::fs::copy(&args.schema, &schema_dest)
        .with_context(|| format!("Failed to copy schema from {:?}", args.schema))?;
    tracing::info!("Copied schema to: {}", schema_dest.display());

    // Generate proto files for Kafka source
    if source_type == SourceType::Kafka {
        let proto_dir = output_dir.join("config").join("proto");
        std::fs::create_dir_all(&proto_dir)
            .with_context(|| format!("Failed to create proto directory: {proto_dir:?}"))?;

        for table in &schema.tables {
            let proto_content = generate_proto_for_table(table, "loadtest");
            let proto_path = proto_dir.join(format!("{}.proto", table.name));
            std::fs::write(&proto_path, &proto_content)
                .with_context(|| format!("Failed to write proto file: {proto_path:?}"))?;
            tracing::info!("Generated proto file: {}", proto_path.display());
        }
    }

    tracing::info!(
        "Configuration generated successfully in: {}",
        output_dir.display()
    );
    tracing::info!("");
    tracing::info!("Next steps:");
    if platforms.contains(&Platform::DockerCompose) {
        tracing::info!("  Docker Compose:");
        tracing::info!(
            "    cd {:?} && docker-compose -f docker-compose.loadtest.yml up",
            output_dir
        );
    }
    if platforms.contains(&Platform::Kubernetes) {
        tracing::info!("  Kubernetes:");
        tracing::info!("    kubectl apply -f {:?}/kubernetes/", output_dir);
    }

    Ok(())
}
