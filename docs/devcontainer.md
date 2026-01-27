# surreal-sync devcontainer

This project uses a devcontainer with Docker Compose that provides all necessary database services for development and testing.

## Setup

1. Open the project in VS Code/Cursor
2. Select "Reopen in Container" when prompted
3. All services start automatically via Docker Compose
4. Wait for initialization to complete (MongoDB replica set setup, etc.)

## Development Workflow

Run `make test` whenever you modify the implementation.

See [Makefile](../Makefile) for more information.

## Available Services

From within the devcontainer, use Docker network hostnames to access databases used in tests:

```bash
# Connect to services using network names
mongo mongodb:27017
psql -h postgresql -U postgres testdb
mysql -h mysql -u root -p testdb
cypher-shell -a bolt://neo4j:7687 -u neo4j -p password

# There are also `neo4j-test1:7687` through `neo4j-test6:7687` for test isolation
```

Port forwarding is intentionally disabled to avoid conflicts with host services.

## Troubleshooting

If you encounter connection issues, double check that you are using service hostnames (`mongodb`, `postgresql`, etc.) not `localhost`.

Still not working? Go back to your host and run `docker ps`, and see if service containers are running. If not, run `docker ps -a` and look into failed servicve container logs for debugging.

To restart the service containers, try `Reopen in Container` first, then `Rebuild Container` if reopening didn't work.
