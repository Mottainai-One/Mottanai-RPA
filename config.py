import os

from dotenv import load_dotenv


load_dotenv()


def get_legacy_db_config():
    """Return the connection settings for the legacy database."""
    return {
        "host": os.getenv("LEGACY_DB_HOST", "localhost"),
        "port": os.getenv("LEGACY_DB_PORT", "5432"),
        "dbname": os.getenv("LEGACY_DB_NAME"),
        "user": os.getenv("LEGACY_DB_USER"),
        "password": os.getenv("LEGACY_DB_PASSWORD"),
        "sslmode": os.getenv("LEGACY_DB_SSLMODE", "require"),
    }


def get_operational_db_config():
    """Return the connection settings for the operational database."""
    return {
        "host": os.getenv("OPERATIONAL_DB_HOST", "localhost"),
        "port": os.getenv("OPERATIONAL_DB_PORT", "5432"),
        "dbname": os.getenv("OPERATIONAL_DB_NAME"),
        "user": os.getenv("OPERATIONAL_DB_USER"),
        "password": os.getenv("OPERATIONAL_DB_PASSWORD"),
        "sslmode": os.getenv("OPERATIONAL_DB_SSLMODE", "require"),
    }


def get_analytics_db_config():
    """Return the independent analytics database connection settings."""
    return {
        "host": os.getenv("ANALYTICS_DB_HOST", "localhost"),
        "port": os.getenv("ANALYTICS_DB_PORT", "5432"),
        "dbname": os.getenv("ANALYTICS_DB_NAME"),
        "user": os.getenv("ANALYTICS_DB_USER"),
        "password": os.getenv("ANALYTICS_DB_PASSWORD"),
        "sslmode": os.getenv("ANALYTICS_DB_SSLMODE", "require"),
    }
