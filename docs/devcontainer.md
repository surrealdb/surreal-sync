This project is designed to work with the provided devcontainer that includes:
- SurrealDB server (localhost:8000)
- MongoDB server (localhost:27017)
- Neo4j server (localhost:7474/7687)

To use the devcontainer:

1. Open the project in VS Code/Cursor
2. Select "Reopen in Container" when prompted
3. All services will start automatically

Once the devcontainer gets built, you can run:

- `cargo test` for running all the unit and integration tests
- `cargo build` for building binaries
