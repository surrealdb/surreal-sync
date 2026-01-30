//! Logging utilities for loadtest populate handlers.

/// Mask password in connection string for safe logging.
pub fn mask_connection_password(conn_str: &str) -> String {
    // Pattern: protocol://user:password@host...
    // Replace password portion with ***
    if let Some(at_pos) = conn_str.find('@') {
        if let Some(colon_pos) = conn_str[..at_pos].rfind(':') {
            let protocol_end = conn_str.find("://").map(|p| p + 3).unwrap_or(0);
            if colon_pos > protocol_end {
                return format!("{}:***{}", &conn_str[..colon_pos], &conn_str[at_pos..]);
            }
        }
    }
    conn_str.to_string()
}
