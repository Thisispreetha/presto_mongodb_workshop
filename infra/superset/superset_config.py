import os

ROW_LIMIT = 50000
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "gdelt_lakehouse_workshop_secret_key_2026_xyz!")

SQLALCHEMY_DATABASE_URI = (
    f"postgresql://{os.environ.get('POSTGRES_USER', 'postgres')}:"
    f"{os.environ.get('POSTGRES_PASSWORD', 'postgres')}@"
    f"{os.environ.get('POSTGRES_HOST', 'postgres')}:"
    f"{os.environ.get('POSTGRES_PORT', '5432')}/"
    f"{os.environ.get('SUPERSET_DB', 'superset')}"
)

# Enable CORS and Feature Flags
ENABLE_PROXY_FIX = True
FEATURE_FLAGS = {
    "ALERT_REPORTS": False,
    "DYNAMIC_PLUGINS": False,
}

# Auto-refresh interval options
CACHE_DEFAULT_TIMEOUT = 10
