from dataclasses import dataclass
import os

from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    source_schema: str = os.getenv("SOURCE_SCHEMA", "public")
    source_table: str = os.getenv("SOURCE_TABLE", "")
    source_incremental_column: str = os.getenv("SOURCE_INCREMENTAL_COLUMN", "updated_at")
    destination_schema: str = os.getenv("DESTINATION_SCHEMA", "integration")
    destination_table: str = os.getenv("DESTINATION_TABLE", "")
    batch_size: int = int(os.getenv("BATCH_SIZE", "1000"))
    max_retries: int = int(os.getenv("MAX_RETRIES", "3"))
    dry_run: bool = os.getenv("DRY_RUN", "true").lower() in {"1", "true", "yes"}
