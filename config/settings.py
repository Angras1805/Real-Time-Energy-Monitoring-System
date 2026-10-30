import os
from pathlib import Path
from dotenv import load_dotenv

# Base project directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Load environment variables from .env file
load_dotenv(BASE_DIR / ".env")

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "smartmeter-events")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "smartmeter-processor-group")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")
PRODUCER_DELAY_SECONDS = float(os.getenv("PRODUCER_DELAY_SECONDS", "0.5"))

# MySQL Database Configuration
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "smartmeter")

# Machine Learning & Analytics Settings
MODEL_DIR = BASE_DIR / "models"
MODEL_PATH = MODEL_DIR / "anomaly_model.pkl"
SLIDING_WINDOW_SIZE = int(os.getenv("SLIDING_WINDOW_SIZE", "30"))
CONTAMINATION_RATE = float(os.getenv("CONTAMINATION_RATE", "0.04"))
RANDOM_STATE = int(os.getenv("RANDOM_STATE", "42"))

# Data Paths
DATA_DIR = BASE_DIR / "data"
DATA_CSV_PATH = DATA_DIR / "smart_meter.csv"

# Dashboard Settings
DASHBOARD_REFRESH_INTERVAL_MS = int(os.getenv("DASHBOARD_REFRESH_INTERVAL_MS", "2000"))
