import sys
import json
import time
import signal
import logging
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PRODUCER_DELAY_SECONDS,
    DATA_CSV_PATH,
)
from utils.data_preprocessing import stream_csv_rows, load_or_create_dataset

logger = logging.getLogger("KafkaEnergyProducer")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )


class SmartMeterProducer:
    """Streams cleaned smart meter time-series readings to Apache Kafka as JSON events."""

    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS, topic: str = KAFKA_TOPIC):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.running = True
        self.producer = None
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        logger.info("\nReceived shutdown signal. Flushing pending messages and closing Kafka Producer...")
        self.running = False

    def connect(self, max_retries: int = 5, retry_interval: float = 2.0) -> bool:
        """Connects to Kafka broker with automatic retry logic."""
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Connecting to Kafka broker at {self.bootstrap_servers} (Attempt {attempt}/{max_retries})...")
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    retries=3,
                    acks="all",
                    linger_ms=10,
                )
                logger.info("Successfully connected to Apache Kafka.")
                return True
            except NoBrokersAvailable:
                logger.warning(f"Kafka broker not available at {self.bootstrap_servers}. Retrying in {retry_interval}s...")
                time.sleep(retry_interval)
            except Exception as err:
                logger.error(f"Unexpected error connecting to Kafka: {err}")
                time.sleep(retry_interval)
        logger.error(f"Failed to connect to Kafka broker after {max_retries} attempts.")
        return False

    def publish_events(self, csv_path: Path = DATA_CSV_PATH, delay_sec: float = PRODUCER_DELAY_SECONDS, max_events: int = 0):
        """
        Streams records from the dataset CSV to Kafka topic.
        Continues in an infinite loop (restarting dataset if reached end) to simulate continuous live streaming.
        """
        if not csv_path.exists():
            logger.info(f"Dataset CSV {csv_path} not found. Triggering download/generator...")
            load_or_create_dataset(csv_path)

        if not self.producer:
            if not self.connect():
                logger.error("Producer connection failed. Exiting.")
                return

        logger.info(f"Starting continuous streaming to topic '{self.topic}' with delay={delay_sec}s...")
        published_count = 0
        loop_count = 1

        while self.running:
            logger.info(f"--- Commencing dataset playback loop #{loop_count} ---")
            for record in stream_csv_rows(csv_path):
                if not self.running:
                    break

                try:
                    # Partition key can be meter/household ID or reading timestamp
                    event_key = "household-01"
                    future = self.producer.send(self.topic, key=event_key, value=record)
                    # Asynchronously handle ack errors
                    future.add_errback(self._on_send_error, record)

                    published_count += 1
                    if published_count % 10 == 0 or published_count <= 5:
                        logger.info(
                            f"[Pub #{published_count:05d}] Timestamp: {record['reading_time']} | "
                            f"Active Power: {record['global_active_power']:.3f} kW | "
                            f"Voltage: {record['voltage']:.1f} V | Intensity: {record['global_intensity']:.1f} A"
                        )

                    if max_events > 0 and published_count >= max_events:
                        logger.info(f"Reached requested max_events limit ({max_events}). Stopping.")
                        self.running = False
                        break

                    time.sleep(delay_sec)
                except KafkaError as kerr:
                    logger.error(f"Kafka error publishing event: {kerr}")
                except Exception as err:
                    logger.error(f"Unexpected error publishing event: {err}")

            loop_count += 1

        self.close()

    def _on_send_error(self, record, exc):
        logger.error(f"Delivery failed for record at {record.get('reading_time')}: {exc}")

    def close(self):
        """Flushes and closes Kafka producer connection gracefully."""
        if self.producer:
            try:
                logger.info("Flushing Kafka producer buffer...")
                self.producer.flush(timeout=5)
                self.producer.close()
                logger.info("Kafka Producer closed gracefully.")
            except Exception as err:
                logger.error(f"Error during producer shutdown: {err}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Smart Meter Real-Time Kafka Producer")
    parser.add_argument("--delay", type=float, default=PRODUCER_DELAY_SECONDS, help="Delay between messages in seconds")
    parser.add_argument("--max-events", type=int, default=0, help="Max events to send (0 = infinite continuous stream)")
    args = parser.parse_args()

    producer = SmartMeterProducer()
    producer.publish_events(delay_sec=args.delay, max_events=args.max_events)


if __name__ == "__main__":
    main()
