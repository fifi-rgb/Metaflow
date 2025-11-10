# Docker development helpers

Use the Makefile in this directory for common operations.

Dev compose:

From the `docker` directory run:

```powershell
make build
make up
```

Or use the dev compose to mount source:

```powershell
docker compose -f docker-compose.dev.yml up --build
```
