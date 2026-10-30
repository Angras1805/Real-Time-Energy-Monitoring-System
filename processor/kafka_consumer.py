import sys
import json
import time
import signal
import logging
from collections import deque
from pathlib import Path
from typing import Optional, Deque

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import joblib
import numpy as np
import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_CONSUMER_GROUP,
    KAFKA_AUTO_OFFSET_RESET,
    MODEL_PATH,
    SLIDING_WINDOW_SIZE,
)
from database.db_connection import db_manager

logger = logging.getLogger("KafkaStreamProcessor")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


class SmartMeterStreamProcessor:
    """
    Consumes live electricity reading events from Kafka, maintains a 30-event sliding window,
    computes rolling metrics and cumulative energy, executes Isolation Forest anomaly inference,
    and persists processed data to MySQL.
    """

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC,
        group_id: str = KAFKA_CONSUMER_GROUP,
        window_size: int = SLIDING_WINDOW_SIZE,
        model_path: Path = MODEL_PATH,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.window_size = window_size
        self.model_path = model_path

        # Stream State & Sliding Window
        self.sliding_window: Deque[float] = deque(maxlen=self.window_size)
        self.current_power: float = 0.0
        self.rolling_avg: float = 0.0
        self.peak_power: float = 0.0
        self.total_energy: float = 0.0  # in kWh
        self.total_readings: int = 0
        self.total_anomalies: int = 0

        self.running = True
        self.consumer: Optional[KafkaConsumer] = None
        self.ml_model = None

        self._setup_signal_handlers()
        self._load_ml_model()
        self._sync_state_from_db()

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        logger.info("\nReceived shutdown signal. Stopping Kafka Stream Consumer gracefully...")
        self.running = False

    def _load_ml_model(self):
        """Loads the pre-trained Isolation Forest model from disk."""
        if not self.model_path.exists():
            logger.warning(f"ML model file not found at {self.model_path}. Anomaly predictions will use statistical fallback.")
            self.ml_model = None
            return

        try:
            self.ml_model = joblib.load(self.model_path)
            logger.info(f"Successfully loaded trained Isolation Forest model from {self.model_path}")
        except Exception as err:
            logger.error(f"Failed to load ML model from {self.model_path}: {err}. Falling back to statistical outlier detection.")
            self.ml_model = None

    def _sync_state_from_db(self):
        """Initializes state from existing database metrics if present."""
        try:
            metrics = db_manager.get_latest_metrics()
            if metrics:
                self.peak_power = float(metrics.get("peak_power", 0.0) or 0.0)
                self.total_energy = float(metrics.get("total_energy", 0.0) or 0.0)
                self.total_readings = int(metrics.get("total_readings", 0) or 0)
                self.total_anomalies = int(metrics.get("total_anomalies", 0) or 0)
                logger.info(
                    f"Synchronized baseline metrics from MySQL: Readings={self.total_readings:,}, "
                    f"Anomalies={self.total_anomalies:,}, Peak={self.peak_power:.2f} kW, Energy={self.total_energy:.3f} kWh"
                )
        except Exception as err:
            logger.warning(f"Could not initialize metrics from MySQL: {err}")

    def connect(self, max_retries: int = 5, retry_interval: float = 2.0) -> bool:
        """Connects to Kafka broker with retry logic."""
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Connecting Consumer to Kafka at {self.bootstrap_servers} (Attempt {attempt}/{max_retries})...")
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=self.group_id,
                    auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
                    enable_auto_commit=True,
                    auto_commit_interval_ms=1000,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    consumer_timeout_ms=1000,
                )
                logger.info(f"Kafka Consumer connected. Subscribed to topic '{self.topic}'.")
                return True
            except NoBrokersAvailable:
                logger.warning(f"Kafka broker not reachable at {self.bootstrap_servers}. Retrying in {retry_interval}s...")
                time.sleep(retry_interval)
            except Exception as err:
                logger.error(f"Error initializing Kafka Consumer: {err}")
                time.sleep(retry_interval)
        return False

    def predict_anomaly(self, active_power: float, rolling_avg: float) -> int:
        """
        Runs ML anomaly inference using Isolation Forest.
        Returns 1 if anomalous, 0 if normal.
        """
        if self.ml_model is not None:
            try:
                # Provide named DataFrame to avoid sklearn feature name warning
                feature_df = pd.DataFrame(
                    [[active_power, rolling_avg]],
                    columns=["global_active_power", "rolling_avg_30"],
                )
                prediction = self.ml_model.predict(feature_df)[0]  # 1 = Normal, -1 = Anomaly
                return 1 if prediction == -1 else 0
            except Exception as err:
                logger.error(f"Inference error with Isolation Forest: {err}")

        # High-reliability statistical fallback: 2.5x rolling average deviation or > 6.5 kW
        if active_power > 6.5 or (rolling_avg > 0.5 and (active_power > 2.8 * rolling_avg or active_power < 0.1 * rolling_avg)):
            return 1
        return 0

    def process_stream(self, max_records: int = 0):
        """
        Continuously polls Kafka for records, updates analytics sliding window,
        runs ML anomaly detection, and stores results into MySQL.
        """
        if not self.consumer:
            if not self.connect():
                logger.error("Consumer connection failed. Exiting.")
                return

        logger.info("=== Smart Meter Stream Processor Running ===")
        logger.info(f"Sliding Window Size: {self.window_size} readings")
        logger.info(f"Listening for events on '{self.topic}'...")

        processed_session = 0

        while self.running:
            try:
                # Poll messages in batches with timeout
                msg_pack = self.consumer.poll(timeout_ms=1000)
                if not msg_pack:
                    continue

                for topic_partition, messages in msg_pack.items():
                    for message in messages:
                        if not self.running:
                            break

                        record = message.value
                        if not isinstance(record, dict) or "global_active_power" not in record:
                            logger.warning(f"Malformed or unexpected message skipped: {record}")
                            continue

                        reading_time = str(record.get("reading_time", time.strftime("%Y-%m-%d %H:%M:%S")))
                        power = float(record["global_active_power"])

                        # 1. Update sliding window
                        self.sliding_window.append(power)
                        self.rolling_avg = float(np.mean(self.sliding_window))
                        self.current_power = power

                        # 2. Update peak consumption
                        if power > self.peak_power:
                            self.peak_power = power

                        # 3. Calculate cumulative energy (kWh)
                        # Assuming 1-minute delta: energy (kWh) = power (kW) * (1 / 60) hours
                        self.total_energy += power * (1.0 / 60.0)

                        # 4. ML Anomaly Prediction
                        is_anomaly = self.predict_anomaly(power, self.rolling_avg)

                        # 5. Counters
                        self.total_readings += 1
                        processed_session += 1
                        if is_anomaly == 1:
                            self.total_anomalies += 1

                        # 6. Database Persistence
                        # Insert individual reading
                        db_manager.insert_reading(
                            reading_time=reading_time,
                            global_active_power=power,
                            rolling_avg_30=self.rolling_avg,
                            is_anomaly=is_anomaly,
                        )

                        # Update aggregated metrics in meter_metrics table
                        db_manager.update_metrics(
                            current_power=self.current_power,
                            rolling_average=self.rolling_avg,
                            peak_power=self.peak_power,
                            total_energy=self.total_energy,
                            total_readings=self.total_readings,
                            total_anomalies=self.total_anomalies,
                        )

                        # Logging
                        status_tag = "[🚨 ANOMALY ALERT]" if is_anomaly else "[NORMAL]"
                        if is_anomaly or processed_session % 10 == 0 or processed_session <= 5:
                            logger.info(
                                f"{status_tag} Event #{self.total_readings:05d} | Time: {reading_time} | "
                                f"Power: {power:.3f} kW | Rolling-30 Avg: {self.rolling_avg:.3f} kW | "
                                f"Peak: {self.peak_power:.2f} kW | Total Energy: {self.total_energy:.3f} kWh | "
                                f"Total Anomalies: {self.total_anomalies}"
                            )

                        if max_records > 0 and processed_session >= max_records:
                            logger.info(f"Processed requested max records ({max_records}). Stopping.")
                            self.running = False
                            break

            except Exception as err:
                logger.error(f"Error during stream consumption: {err}", exc_info=False)
                time.sleep(0.5)

        self.close()

    def close(self):
        """Closes Kafka consumer and database connection cleanly."""
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka Consumer closed.")
            except Exception:
                pass
        db_manager.close()
        logger.info("Stream Processor shut down gracefully.")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Smart Meter Real-Time Stream Processor")
    parser.add_argument("--max-records", type=int, default=0, help="Max records to consume (0 = infinite)")
    args = parser.parse_args()

    processor = SmartMeterStreamProcessor()
    processor.process_stream(max_records=args.max_records)


if __name__ == "__main__":
    main()
