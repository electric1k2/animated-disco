import os
from dotenv import load_dotenv

load_dotenv()

# Bot Configuration
BOT_TOKEN = os.getenv("BOT_TOKEN", "8322142454:AAEtMfaeg6h2-IS_D6XovcuC6iXy83ATRVY")
ADMIN_ID = int(os.getenv("ADMIN_ID", "7011309417"))  # Will be updated to elweshaq_1's ID
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "elweshaq_1")  # Admin username without @
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")

# Database Configuration
DATABASE_URL = os.getenv("DATABASE_URL")

# Application Settings
RESERVATION_TIMEOUT_MIN = int(os.getenv("RESERVATION_TIMEOUT_MIN", "20"))
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "5"))
DEFAULT_REWARD_AMOUNT = float(os.getenv("DEFAULT_REWARD_AMOUNT", "5.0"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "10"))

# Provider API Settings
PROVIDER_API_TIMEOUT = int(os.getenv("PROVIDER_API_TIMEOUT", "30"))

# Group Message Processing Settings
HMAC_SECRET = os.getenv("HMAC_SECRET", "default_hmac_secret_key")
