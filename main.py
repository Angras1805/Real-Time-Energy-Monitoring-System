import sys
import os
import time
import argparse
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

PYTHON_EXEC = sys.executable


def run_db_setup():
    print("=" * 60)
    print("Layer 5: Initializing MySQL Database & Schema")
    print("=" * 60)
    from database.db_connection import db_manager
    if db_manager.check_connection():
        print(f"Connected to MySQL on host: {db_manager.host}:{db_manager.port}")
        # Run schema file
        schema_file = PROJECT_ROOT / "database" / "schema.sql"
        if schema_file.exists():
            with open(schema_file, "r") as f:
                statements = [s.strip() for s in f.read().split(";") if s.strip()]
            conn = db_manager.get_connection()
            with conn.cursor() as cursor:
                for stmt in statements:
                    cursor.execute(stmt)
            print("Database 'smartmeter' and tables successfully verified.")
        else:
            print("Error: schema.sql not found.")
    else:
        print("Failed to connect to MySQL. Ensure MySQL server is running.")


def run_training():
    print("=" * 60)
    print("Layer 4: Training Isolation Forest Anomaly Detection Model")
    print("=" * 60)
    from ml.train_model import train_isolation_forest
    train_isolation_forest()


def run_producer(delay: float = 0.5, max_events: int = 0):
    print("=" * 60)
    print("Layer 2: Launching Apache Kafka Producer")
    print("=" * 60)
    from producer.kafka_producer import SmartMeterProducer
    producer = SmartMeterProducer()
    producer.publish_events(delay_sec=delay, max_events=max_events)


def run_consumer(max_records: int = 0):
    print("=" * 60)
    print("Layer 3: Launching Kafka Consumer & Stream Processor")
    print("=" * 60)
    from processor.kafka_consumer import SmartMeterStreamProcessor
    processor = SmartMeterStreamProcessor()
    processor.process_stream(max_records=max_records)


def run_dashboard():
    print("=" * 60)
    print("Layer 6: Launching Streamlit Real-Time Analytics Dashboard")
    print("=" * 60)
    app_path = PROJECT_ROOT / "dashboard" / "app.py"
    subprocess.run([PYTHON_EXEC, "-m", "streamlit", "run", str(app_path), "--server.port=8501", "--server.headless=true"])


def main():
    parser = argparse.ArgumentParser(
        description="Smart Energy Monitoring & Anomaly Detection System - Master Controller"
    )
    parser.add_argument(
        "action",
        choices=["setup-db", "train", "producer", "consumer", "dashboard", "full-pipeline"],
        help="Pipeline layer action to execute",
    )
    parser.add_argument("--delay", type=float, default=0.5, help="Producer event delay in seconds")
    parser.add_argument("--max-events", type=int, default=0, help="Max producer events (0 = infinite)")
    parser.add_argument("--max-records", type=int, default=0, help="Max consumer records (0 = infinite)")

    args = parser.parse_args()

    if args.action == "setup-db":
        run_db_setup()
    elif args.action == "train":
        run_training()
    elif args.action == "producer":
        run_producer(delay=args.delay, max_events=args.max_events)
    elif args.action == "consumer":
        run_consumer(max_records=args.max_records)
    elif args.action == "dashboard":
        run_dashboard()
    elif args.action == "full-pipeline":
        print("Starting full end-to-end pipeline...")
        run_db_setup()
        run_training()
        print("Setup and training completed. You can now start consumer, producer, and dashboard in separate terminals.")


if __name__ == "__main__":
    main()
