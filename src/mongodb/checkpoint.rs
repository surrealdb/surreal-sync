//! MongoDB checkpoint management
//!
//! This module provides utilities for obtaining and managing MongoDB resume tokens
//! for change stream-based incremental synchronization.

use anyhow::Result;
use mongodb::{
    options::{ChangeStreamOptions, FullDocumentType},
    Client as MongoClient,
};

/// Get current resume token from MongoDB change stream using existing client
pub async fn get_resume_token(client: &MongoClient, database: &str) -> Result<Vec<u8>> {
    let database = client.database(database);

    // Create a change stream with no pipeline to get current token
    let options = ChangeStreamOptions::builder()
        .full_document(Some(FullDocumentType::UpdateLookup))
        .build();

    let change_stream = database.watch().with_options(options).await?;

    // Get the resume token from the stream
    if let Some(token) = change_stream.resume_token() {
        let bytes = bson::to_vec(&token)?;
        return Ok(bytes);
    }

    // If no token is immediately available, return an error
    Err(anyhow::anyhow!(
        "No resume token available from change stream"
    ))
}
