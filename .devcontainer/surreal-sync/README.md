# SurrealSync DevContainer

This devcontainer provides a environment for surreal-sync, including:

- Rust development environment with cargo and rust-analyzer
- SurrealDB server running on port 8000
- MongoDB server running on port 27017 
- Neo4j server running on ports 7474 (HTTP) and 7687 (Bolt)

## Services

### SurrealDB
- **URL**: http://localhost:8000
- **Credentials**: root/root
- **Command**: `surreal sql --conn http://localhost:8000 --user root --pass root --ns test --db test`

### MongoDB
- **URL**: mongodb://localhost:27017
- **Credentials**: root/root
- **Database**: sample_db (pre-populated with test data)
- **Command**: `mongosh mongodb://root:root@localhost:27017/sample_db`

### Neo4j
- **Web Interface**: http://localhost:7474
- **Bolt URL**: bolt://localhost:7687
- **Credentials**: neo4j/password
- **Command**: `cypher-shell -a bolt://localhost:7687 -u neo4j -p password`

## Usage

1. Open the project root in VS Code/Cursor
2. Select "Reopen in Container"
