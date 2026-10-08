import logging

from pipeline import run_pipeline
from settings import Settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

if __name__ == "__main__":
    settings = Settings()
    logging.info("Iniciando Mottainai RPA (dry_run=%s)", settings.dry_run)
    result = run_pipeline(settings)
    logging.info("Concluído: %s", result)
