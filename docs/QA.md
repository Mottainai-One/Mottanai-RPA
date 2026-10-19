# RPA verification in QA

The QA image runs the repository's tests without database credentials or network access. This verifies mapping and orchestration logic without writing to the legacy, operational or analytical databases.

Build and run locally:

```bash
docker build -f Dockerfile.qa -t mottainai-rpa:qa .
docker run --rm --network none mottainai-rpa:qa
```

The production schedule and credentials in `legacy-to-operational.yml` are separate. Do not point scheduled QA tasks at the real legacy or operational databases. A future integration test needs disposable PostgreSQL sources, synthetic fixtures and explicit checks for idempotency before it can be scheduled.
