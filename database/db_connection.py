import sys
import logging
import time
from pathlib import Path
from typing import Dict, Any, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pymysql
from pymysql.cursors import DictCursor

from config.settings import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
)

logger = logging.getLogger("DatabaseManager")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


class DatabaseManager:
    """Handles MySQL database connections, transactional executions, and queries."""

    def __init__(self):
        self.host = MYSQL_HOST
        self.port = MYSQL_PORT
        self.user = MYSQL_USER
        self.password = MYSQL_PASSWORD
        self.database = MYSQL_DATABASE
        self.conn = None

    def get_connection(self):
        """Establishes or reconnects to MySQL server."""
        try:
            if self.conn is None or not self.conn.open:
                self.conn = pymysql.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    autocommit=True,
                    cursorclass=DictCursor,
                    connect_timeout=10,
                )
            else:
                self.conn.ping(reconnect=True)
            return self.conn
        except pymysql.MySQLError as err:
            logger.error(f"MySQL connection error to {self.host}:{self.port}/{self.database}: {err}")
            raise

    def check_connection(self) -> bool:
        """Returns True if MySQL is reachable, False otherwise."""
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 AS status;")
                res = cursor.fetchone()
                return bool(res and res["status"] == 1)
        except Exception as err:
            logger.warning(f"Database health check failed: {err}")
            return False

    def insert_reading(
        self,
        reading_time: str,
        global_active_power: float,
        rolling_avg_30: float,
        is_anomaly: int,
    ) -> bool:
        """Inserts a single processed time-series meter reading into meter_readings."""
        query = """
            INSERT INTO meter_readings (reading_time, global_active_power, rolling_avg_30, is_anomaly)
            VALUES (%s, %s, %s, %s);
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(
                    query,
                    (reading_time, float(global_active_power), float(rolling_avg_30), int(is_anomaly)),
                )
            return True
        except Exception as err:
            logger.error(f"Failed to insert reading at {reading_time}: {err}")
            return False

    def update_metrics(
        self,
        current_power: float,
        rolling_average: float,
        peak_power: float,
        total_energy: float,
        total_readings: int,
        total_anomalies: int,
    ) -> bool:
        """Updates the singleton metrics record (id=1) in meter_metrics."""
        query = """
            INSERT INTO meter_metrics (id, current_power, rolling_average, peak_power, total_energy, total_readings, total_anomalies)
            VALUES (1, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                current_power = VALUES(current_power),
                rolling_average = VALUES(rolling_average),
                peak_power = VALUES(peak_power),
                total_energy = VALUES(total_energy),
                total_readings = VALUES(total_readings),
                total_anomalies = VALUES(total_anomalies),
                updated_at = CURRENT_TIMESTAMP;
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(
                    query,
                    (
                        float(current_power),
                        float(rolling_average),
                        float(peak_power),
                        float(total_energy),
                        int(total_readings),
                        int(total_anomalies),
                    ),
                )
            return True
        except Exception as err:
            logger.error(f"Failed to update meter metrics: {err}")
            return False

    def get_latest_metrics(self) -> Optional[Dict[str, Any]]:
        """Fetches the latest summary metrics from meter_metrics table."""
        query = "SELECT * FROM meter_metrics WHERE id = 1 LIMIT 1;"
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchone()
        except Exception as err:
            logger.error(f"Failed to fetch latest metrics: {err}")
            return None

    def get_recent_readings(self, limit: int = 200) -> List[Dict[str, Any]]:
        """Retrieves the most recent meter readings ordered by timestamp ascending."""
        query = f"""
            SELECT reading_pk, reading_time, global_active_power, rolling_avg_30, is_anomaly, created_at
            FROM (
                SELECT reading_pk, reading_time, global_active_power, rolling_avg_30, is_anomaly, created_at
                FROM meter_readings
                ORDER BY reading_pk DESC
                LIMIT {int(limit)}
            ) sub
            ORDER BY reading_pk ASC;
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
        except Exception as err:
            logger.error(f"Failed to fetch recent readings: {err}")
            return []

    def get_anomaly_records(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Retrieves recent detected anomalous readings."""
        query = f"""
            SELECT reading_pk, reading_time, global_active_power, rolling_avg_30, created_at
            FROM meter_readings
            WHERE is_anomaly = 1
            ORDER BY reading_pk DESC
            LIMIT {int(limit)};
        """
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
        except Exception as err:
            logger.error(f"Failed to fetch anomaly records: {err}")
            return []

    def close(self):
        """Closes the active database connection."""
        if self.conn and self.conn.open:
            try:
                self.conn.close()
                logger.info("MySQL connection closed gracefully.")
            except Exception:
                pass


# Singleton instance helper
db_manager = DatabaseManager()
