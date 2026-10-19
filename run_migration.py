import logging
import os

from relational_migration import migrate
from settings import Settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

if __name__ == "__main__":
    dry_run = os.getenv("MIGRATION_DRY_RUN", "true").lower() in {"1", "true", "yes"}
    result = migrate(Settings(), dry_run=dry_run)
    print({"dry_run": dry_run, "records_seen": result})
