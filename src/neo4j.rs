use chrono::DateTime;
use neo4rs::{ConfigBuilder, Graph, Query};
use std::collections::HashMap;
use surrealdb::{engine::any::connect, Surreal};

use crate::{BindableValue, SourceOpts, SurrealOpts};

pub async fn migrate_from_neo4j(
    from_opts: SourceOpts,
    to_namespace: String,
    to_database: String,
    to_opts: SurrealOpts,
) -> anyhow::Result<()> {
    tracing::info!("Starting Neo4j migration");
    tracing::debug!(
        "migrate_from_neo4j function called with URI: {}",
        from_opts.source_uri
    );

    // Connect to Neo4j
    tracing::debug!("Connecting to Neo4j at: {}", from_opts.source_uri);
    let config = ConfigBuilder::default()
        .uri(&from_opts.source_uri)
        .user(
            from_opts
                .source_username
                .unwrap_or_else(|| "neo4j".to_string()),
        )
        .password(
            from_opts
                .source_password
                .unwrap_or_else(|| "password".to_string()),
        )
        .db(from_opts
            .source_database
            .unwrap_or_else(|| "neo4j".to_string()))
        .build()?;

    let graph = Graph::connect(config)?;
    tracing::debug!("Neo4j connection established");

    // Connect to SurrealDB
    let surreal_endpoint = to_opts
        .surreal_endpoint
        .replace("http://", "ws://")
        .replace("https://", "wss://");
    tracing::debug!("Connecting to SurrealDB at: {}", surreal_endpoint);
    let surreal = connect(surreal_endpoint).await?;
    tracing::debug!("SurrealDB connection established");

    tracing::debug!(
        "Signing in to SurrealDB with username: {}",
        to_opts.surreal_username
    );
    surreal
        .signin(surrealdb::opt::auth::Root {
            username: &to_opts.surreal_username,
            password: &to_opts.surreal_password,
        })
        .await?;
    tracing::debug!("SurrealDB signin successful");

    tracing::debug!(
        "Using SurrealDB namespace: {} and database: {}",
        to_namespace,
        to_database
    );
    surreal.use_ns(&to_namespace).use_db(&to_database).await?;
    tracing::debug!("SurrealDB namespace and database selected");

    tracing::info!("Connected to both Neo4j and SurrealDB");

    let mut total_migrated = 0;

    // Migrate nodes first
    total_migrated += migrate_neo4j_nodes(&graph, &surreal, &to_opts).await?;

    // Then migrate relationships
    total_migrated += migrate_neo4j_relationships(&graph, &surreal, &to_opts).await?;

    tracing::info!(
        "Neo4j migration completed: {} total items migrated",
        total_migrated
    );
    Ok(())
}

/// Migrate all nodes from Neo4j to SurrealDB
async fn migrate_neo4j_nodes(
    graph: &Graph,
    surreal: &Surreal<surrealdb::engine::any::Any>,
    to_opts: &SurrealOpts,
) -> anyhow::Result<usize> {
    tracing::info!("Starting Neo4j nodes migration");

    // Get all distinct node labels first
    let label_query = Query::new("MATCH (n) RETURN DISTINCT labels(n) as labels".to_string());
    let mut result = graph.execute(label_query).await?;

    let mut all_labels = std::collections::HashSet::new();
    while let Some(row) = result.next().await? {
        let labels: Vec<String> = row.get("labels")?;
        for label in labels {
            all_labels.insert(label);
        }
    }

    tracing::info!("Found {} distinct node labels", all_labels.len());
    tracing::debug!("Node labels: {:?}", all_labels);

    let mut total_migrated = 0;

    // Process each label separately to create proper SurrealDB tables
    for label in all_labels {
        tracing::info!("Migrating nodes with label: {}", label);

        let node_query = Query::new(
            "MATCH (n) WHERE $label IN labels(n) RETURN n, id(n) as node_id".to_string(),
        )
        .param("label", label.clone());
        let mut node_result = graph.execute(node_query).await?;

        let mut batch = Vec::new();
        let mut processed = 0;

        while let Some(row) = node_result.next().await? {
            let node: neo4rs::Node = row.get("n")?;
            let node_id: i64 = row.get("node_id")?;

            // Convert Neo4j node to SurrealDB format
            let surreal_record = convert_neo4j_node_to_bindable(node, node_id, &label)?;
            let record_id = format!("{}:{}", label.to_lowercase(), node_id);

            if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                tracing::debug!(
                    "Converted Neo4j node to SurrealDB record (id: {}): {:?}",
                    record_id,
                    surreal_record
                );
            }

            batch.push((record_id, surreal_record));

            if batch.len() >= to_opts.batch_size {
                tracing::debug!(
                    "Batch size reached ({}), processing batch for label: {}",
                    batch.len(),
                    label
                );
                if !to_opts.dry_run {
                    crate::migrate_batch(surreal, &label.to_lowercase(), &batch).await?;
                } else {
                    tracing::debug!("Dry-run mode: skipping actual migration of batch");
                }
                processed += batch.len();
                total_migrated += batch.len();
                tracing::info!("Processed {} nodes with label '{}'", processed, label);
                batch.clear();
            }
        }

        // Process remaining nodes in the last batch
        if !batch.is_empty() {
            tracing::debug!(
                "Processing final batch of {} nodes for label: {}",
                batch.len(),
                label
            );
            if !to_opts.dry_run {
                crate::migrate_batch(surreal, &label.to_lowercase(), &batch).await?;
            } else {
                tracing::debug!("Dry-run mode: skipping actual migration of final batch");
            }
            processed += batch.len();
            total_migrated += batch.len();
        }

        tracing::info!(
            "Completed migration of label '{}': {} nodes",
            label,
            processed
        );
    }

    tracing::info!(
        "Completed Neo4j nodes migration: {} total nodes",
        total_migrated
    );
    Ok(total_migrated)
}

/// Migrate all relationships from Neo4j to SurrealDB
async fn migrate_neo4j_relationships(
    graph: &Graph,
    surreal: &Surreal<surrealdb::engine::any::Any>,
    to_opts: &SurrealOpts,
) -> anyhow::Result<usize> {
    tracing::info!("Starting Neo4j relationships migration");

    // Get all distinct relationship types first
    let type_query = Query::new("MATCH ()-[r]->() RETURN DISTINCT type(r) as rel_type".to_string());
    let mut result = graph.execute(type_query).await?;

    let mut all_types = std::collections::HashSet::new();
    while let Some(row) = result.next().await? {
        let rel_type: String = row.get("rel_type")?;
        all_types.insert(rel_type);
    }

    tracing::info!("Found {} distinct relationship types", all_types.len());
    tracing::debug!("Relationship types: {:?}", all_types);

    let mut total_migrated = 0;

    // Process each relationship type separately
    for rel_type in all_types {
        tracing::info!("Migrating relationships of type: {}", rel_type);

        let rel_query = Query::new(
            "MATCH (start_node)-[r]->(end_node) WHERE type(r) = $rel_type 
             RETURN r, id(r) as rel_id, id(start_node) as start_id, id(end_node) as end_id,
             labels(start_node) as start_labels, labels(end_node) as end_labels"
                .to_string(),
        )
        .param("rel_type", rel_type.clone());
        let mut rel_result = graph.execute(rel_query).await?;

        let mut batch = Vec::new();
        let mut processed = 0;

        while let Some(row) = rel_result.next().await? {
            let relationship: neo4rs::Relation = row.get("r")?;
            let rel_id: i64 = row.get("rel_id")?;
            let start_id: i64 = row.get("start_id")?;
            let end_id: i64 = row.get("end_id")?;
            let start_labels: Vec<String> = row.get("start_labels")?;
            let end_labels: Vec<String> = row.get("end_labels")?;

            // Convert Neo4j relationship to SurrealDB format
            let surreal_record = convert_neo4j_relationship_to_bindable(
                relationship,
                start_id,
                end_id,
                &start_labels,
                &end_labels,
            )?;
            let record_id = format!("{}:{}", rel_type.to_lowercase(), rel_id);

            if std::env::var("SURREAL_SYNC_DEBUG").is_ok() {
                tracing::debug!(
                    "Converted Neo4j relationship to SurrealDB record (id: {}): {:?}",
                    record_id,
                    surreal_record
                );
            }

            batch.push((record_id, surreal_record));

            if batch.len() >= to_opts.batch_size {
                tracing::debug!(
                    "Batch size reached ({}), processing batch for type: {}",
                    batch.len(),
                    rel_type
                );
                if !to_opts.dry_run {
                    crate::migrate_batch(surreal, &rel_type.to_lowercase(), &batch).await?;
                } else {
                    tracing::debug!("Dry-run mode: skipping actual migration of batch");
                }
                processed += batch.len();
                total_migrated += batch.len();
                tracing::info!(
                    "Processed {} relationships of type '{}'",
                    processed,
                    rel_type
                );
                batch.clear();
            }
        }

        // Process remaining relationships in the last batch
        if !batch.is_empty() {
            tracing::debug!(
                "Processing final batch of {} relationships for type: {}",
                batch.len(),
                rel_type
            );
            if !to_opts.dry_run {
                crate::migrate_batch(surreal, &rel_type.to_lowercase(), &batch).await?;
            } else {
                tracing::debug!("Dry-run mode: skipping actual migration of final batch");
            }
            processed += batch.len();
            total_migrated += batch.len();
        }

        tracing::info!(
            "Completed migration of relationship type '{}': {} relationships",
            rel_type,
            processed
        );
    }

    tracing::info!(
        "Completed Neo4j relationships migration: {} total relationships",
        total_migrated
    );
    Ok(total_migrated)
}

/// Convert Neo4j node to bindable HashMap with BindableValue
fn convert_neo4j_node_to_bindable(
    node: neo4rs::Node,
    node_id: i64,
    _label: &str,
) -> anyhow::Result<std::collections::HashMap<String, BindableValue>> {
    let mut bindable_obj = std::collections::HashMap::new();

    // Add neo4j_id as a field (preserve original Neo4j ID)
    bindable_obj.insert("neo4j_id".to_string(), BindableValue::Int(node_id));

    // Add labels as an array
    let labels: Vec<String> = node.labels().into_iter().map(|s| s.to_string()).collect();
    let labels_bindables: Vec<BindableValue> =
        labels.into_iter().map(BindableValue::String).collect();
    bindable_obj.insert("labels".to_string(), BindableValue::Array(labels_bindables));

    // Convert all properties
    for key in node.keys() {
        let value = node.get::<neo4rs::BoltType>(key)?;
        let bindable_value = convert_neo4j_type_to_bindable(value)?;
        bindable_obj.insert(key.to_string(), bindable_value);
    }

    Ok(bindable_obj)
}

/// Convert Neo4j relationship to bindable HashMap with BindableValue
fn convert_neo4j_relationship_to_bindable(
    relationship: neo4rs::Relation,
    start_id: i64,
    end_id: i64,
    start_labels: &[String],
    end_labels: &[String],
) -> anyhow::Result<std::collections::HashMap<String, BindableValue>> {
    let mut bindable_obj = std::collections::HashMap::new();

    // Add SurrealDB RELATE required fields: 'in' and 'out' as Thing types
    let start_table = start_labels
        .first()
        .map(|s| s.to_lowercase())
        .unwrap_or_else(|| "node".to_string());
    let end_table = end_labels
        .first()
        .map(|s| s.to_lowercase())
        .unwrap_or_else(|| "node".to_string());

    let in_thing = surrealdb::sql::Thing::try_from(
        format!("{}:{}", start_table, start_id).as_str(),
    )
    .map_err(|_| {
        anyhow::anyhow!(
            "Failed to create Thing for in field: {}:{}",
            start_table,
            start_id
        )
    })?;
    let out_thing = surrealdb::sql::Thing::try_from(format!("{}:{}", end_table, end_id).as_str())
        .map_err(|_| {
        anyhow::anyhow!(
            "Failed to create Thing for out field: {}:{}",
            end_table,
            end_id
        )
    })?;

    bindable_obj.insert("in".to_string(), BindableValue::Thing(in_thing));
    bindable_obj.insert("out".to_string(), BindableValue::Thing(out_thing));

    // Convert all properties
    for key in relationship.keys() {
        let value = relationship.get::<neo4rs::BoltType>(key)?;
        let bindable_value = convert_neo4j_type_to_bindable(value)?;
        bindable_obj.insert(key.to_string(), bindable_value);
    }

    Ok(bindable_obj)
}

/// Convert Neo4j BoltType to BindableValue
///
/// Currently, this function supports only one of the possible modes of operation:
/// 1. Assume each node has only one label- No dedicated SurrealDB table for all the Neo4j nodes, but rather a SurrealDB table for each Neo4j node label.
/// 2. Assume each node may have two or more labels- A dedicated SurrealDB table for all the Neo4j nodes, plus additional SurrealDB tables for each Neo4j node label.
///
/// This function currently assumes the first mode.
///
/// TODO: Add support for the second mode by allowing the caller to specify the name of the SurrealDB table to use for all the Neo4j nodes.
///
/// Note that we currently assumes local time zone is GMT+0. See for example what we deal with LocalDateTime with and_utc().
/// TODO: Make this configurable, so that we can use the local time zone of surreal-sync, utc, or some specific time zone.
fn convert_neo4j_type_to_bindable(value: neo4rs::BoltType) -> anyhow::Result<BindableValue> {
    match value {
        neo4rs::BoltType::String(s) => Ok(BindableValue::String(s.value)),
        neo4rs::BoltType::Boolean(b) => Ok(BindableValue::Bool(b.value)),
        neo4rs::BoltType::Map(map) => {
            let mut bindables = HashMap::new();
            for (key, val) in map.value {
                let bindable_val = convert_neo4j_type_to_bindable(val)?;
                bindables.insert(key.to_string(), bindable_val);
            }
            Ok(BindableValue::Object(bindables))
        }
        neo4rs::BoltType::Null(_) => Ok(BindableValue::Null),
        neo4rs::BoltType::Integer(i) => Ok(BindableValue::Int(i.value)),
        neo4rs::BoltType::Float(f) => Ok(BindableValue::Float(f.value)),
        neo4rs::BoltType::List(list) => {
            let mut bindables = Vec::new();
            for item in list.value {
                let bindable_val = convert_neo4j_type_to_bindable(item)?;
                bindables.push(bindable_val);
            }
            Ok(BindableValue::Array(bindables))
        }
        neo4rs::BoltType::Node(node) => {
            // We assume that nodes are not stored in Neo4j as node properties.
            // In other words, nodes are processed and converted in more upper function,
            // so we should never encounter a node here.
            Err(anyhow::anyhow!(
                "Node type is not supported for migration from Neo4j to SurrealDB: {:?}",
                node
            ))
        }
        neo4rs::BoltType::Relation(relation) => {
            // We assume that relations are not stored in Neo4j as node propertaies.
            // In other words, nodes are processed and converted in more upper function,
            // so we should never encounter a relation here.
            Err(anyhow::anyhow!(
                "Relation type is not supported for migration from Neo4j to SurrealDB: {:?}",
                relation
            ))
        }
        // "A relationship without start or end node ID. It is used internally for Path serialization."
        // https://neo4j.com/docs/bolt/current/bolt/structure-semantics/#structure-unbound
        neo4rs::BoltType::UnboundedRelation(_unbounded_relation) => {
            // We assume that unbounded relations are not stored in Neo4j, and therefore
            // we never encounter them in the input to surreal-sync.

            // Also note that SurrealDB relation needs in and out, which means we cannot convert
            // unbounded relations to SurrealDB relations anyway.
            // See https://surrealdb.com/docs/surrealql/statements/relate for more details on how SurrealDB's RELATE works.
            Err(anyhow::anyhow!(
                "UnboundedRelation type is not supported for migration from Neo4j to SurrealDB"
            ))
        }
        neo4rs::BoltType::Point2D(point) => {
            // TODO: We need to turn into into https://geojson.org/
            // sr_id is the srid, which is not directly representable in SurrealDB.
            //
            // TODO: Instead of surrealdb::sql::Geometry::Point, we will use a custom object to represent the point,
            // whose fields are:
            // - type: "Point" (See https://surrealdb.com/docs/surrealql/datamodel/geometries#point)
            // - srid: the srid of the point
            // - coordinates: an array of two floats [x, y]
            let mut bindables = HashMap::new();
            bindables.insert(
                "type".to_string(),
                BindableValue::String("Point2D".to_string()),
            );
            bindables.insert("srid".to_string(), BindableValue::Int(point.sr_id.value));
            bindables.insert("x".to_string(), BindableValue::Float(point.x.value));
            bindables.insert("y".to_string(), BindableValue::Float(point.y.value));
            Ok(BindableValue::Object(bindables))
        }
        neo4rs::BoltType::Point3D(point) => {
            // TODO: We need to turn into into https://geojson.org/
            // sr_id is the srid, which is not directly representable in SurrealDB
            //
            // TODO: Note that SurrealDB does not support 3-dimensional points yet,
            // although we use a custom object to represent the 3d point hoping
            // that SurrealDB will support it in the future.
            // The custom object will have the following fields:
            // - type: "Point3D" (This might be changed to a more appropriate name in the future)
            // - srid: the srid of the point
            // - coordinates: an array of three floats [x, y, z]
            let mut bindables = HashMap::new();
            bindables.insert(
                "type".to_string(),
                BindableValue::String("Point3D".to_string()),
            );
            bindables.insert("srid".to_string(), BindableValue::Int(point.sr_id.value));
            bindables.insert("x".to_string(), BindableValue::Float(point.x.value));
            bindables.insert("y".to_string(), BindableValue::Float(point.y.value));
            bindables.insert("z".to_string(), BindableValue::Float(point.z.value));
            Ok(BindableValue::Object(bindables))
        }
        neo4rs::BoltType::Bytes(bytes) => {
            // Convert Neo4j bytes to SurrealDB bytes type
            Ok(BindableValue::Bytes(bytes.value.to_vec()))
        }
        neo4rs::BoltType::Path(_path) => {
            // We assume Path to never appear in the input to surreal-sync, because it is not a stored data type in Neo4j,
            // but rather a result of a query that traverses the graph.
            // See https://neo4j.com/blog/developer/the-power-of-the-path-1/ for more details.
            Err(anyhow::anyhow!(
                "Path type is not supported for migration from Neo4j to SurrealDB"
            ))
        }
        neo4rs::BoltType::Date(date) => {
            let naive_d: chrono::NaiveDate = date.try_into()?;
            // Make this configurable?
            let now = chrono::Local::now();
            let tz = now.offset();
            // Assumes the naivedate is in the specified tz, and produce a UTC datetime
            let naive_dt = chrono::NaiveDateTime::new(
                naive_d,
                chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
            );
            let dt_with_tz: DateTime<chrono::FixedOffset> =
                chrono::DateTime::from_naive_utc_and_offset(naive_dt, tz.to_owned());
            let utc_dt = dt_with_tz.into();
            Ok(BindableValue::DateTime(utc_dt))
        }
        // "An instant capturing the time of day, and the timezone offset in seconds, but not the date"
        // https://neo4j.com/docs/bolt/current/bolt/structure-semantics/#structure-time
        neo4rs::BoltType::Time(time) => {
            let t: (chrono::NaiveTime, chrono::FixedOffset) = time.into();
            let mut obj = HashMap::new();
            // Maybe we should have options to make it internally tagged (as its externally tagged for now by a parallel `type` field)
            // and make the type field value configurable for parsing convenience?
            obj.insert(
                "type".to_string(),
                BindableValue::String("$Neo4jTime".to_string()),
            );
            obj.insert(
                "offset_seconds".to_string(),
                BindableValue::Int(t.1.local_minus_utc() as i64),
            );
            use chrono::Timelike;
            obj.insert("hour".to_string(), BindableValue::Int(t.0.hour() as i64));
            obj.insert(
                "minute".to_string(),
                BindableValue::Int(t.0.minute() as i64),
            );
            obj.insert(
                "second".to_string(),
                BindableValue::Int(t.0.second() as i64),
            );
            obj.insert(
                "nanosecond".to_string(),
                BindableValue::Int(t.0.nanosecond() as i64),
            );
            Ok(BindableValue::Object(obj))
        }
        neo4rs::BoltType::LocalTime(local_time) => {
            let cnt: chrono::NaiveTime = local_time.into();
            let mut obj = HashMap::new();
            // Maybe we should have options to make it internally tagged (as its externally tagged for now by a parallel `type` field)
            // and make the type field value configurable for parsing convenience?
            obj.insert(
                "type".to_string(),
                BindableValue::String("$Neo4jLocalTime".to_string()),
            );
            use chrono::Timelike;
            obj.insert("hour".to_string(), BindableValue::Int(cnt.hour() as i64));
            obj.insert(
                "minute".to_string(),
                BindableValue::Int(cnt.minute() as i64),
            );
            obj.insert(
                "second".to_string(),
                BindableValue::Int(cnt.second() as i64),
            );
            obj.insert(
                "nanosecond".to_string(),
                BindableValue::Int(cnt.nanosecond() as i64),
            );
            Ok(BindableValue::Object(obj))
        }
        neo4rs::BoltType::DateTime(datetime) => {
            let dt: chrono::DateTime<chrono::FixedOffset> = datetime.try_into()?;
            let utc_dt = dt.into();
            Ok(BindableValue::DateTime(utc_dt))
        }
        neo4rs::BoltType::LocalDateTime(local_datetime) => {
            let dt: chrono::NaiveDateTime = local_datetime.try_into()?;
            let utc_dt = dt.and_utc();
            Ok(BindableValue::DateTime(utc_dt))
        }
        neo4rs::BoltType::DateTimeZoneId(datetime_zone_id) => {
            // Convert Neo4j DateTimeZoneId to string representation
            Ok(BindableValue::String(format!("{:?}", datetime_zone_id)))
        }
        neo4rs::BoltType::Duration(duration) => {
            let std_duration: std::time::Duration = duration.into();
            Ok(BindableValue::Duration(std_duration))
        }
    }
}
