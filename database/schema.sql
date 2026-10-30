-- Smart Energy Monitoring and Anomaly Detection Database Schema
-- Database: smartmeter

CREATE DATABASE IF NOT EXISTS smartmeter;
USE smartmeter;

-- Table 1: Individual time-series meter readings with anomaly classifications
CREATE TABLE IF NOT EXISTS meter_readings (
    reading_pk INT AUTO_INCREMENT PRIMARY KEY,
    reading_time DATETIME NOT NULL,
    global_active_power DOUBLE NOT NULL,
    rolling_avg_30 DOUBLE NOT NULL,
    is_anomaly INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_reading_time (reading_time),
    INDEX idx_is_anomaly (is_anomaly)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Table 2: Real-time aggregated system metrics and KPIs (singleton row id=1)
CREATE TABLE IF NOT EXISTS meter_metrics (
    id INT PRIMARY KEY DEFAULT 1,
    current_power DOUBLE NOT NULL DEFAULT 0.0,
    rolling_average DOUBLE NOT NULL DEFAULT 0.0,
    peak_power DOUBLE NOT NULL DEFAULT 0.0,
    total_energy DOUBLE NOT NULL DEFAULT 0.0,
    total_readings INT NOT NULL DEFAULT 0,
    total_anomalies INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Seed default initial metrics record if not present
INSERT INTO meter_metrics (id, current_power, rolling_average, peak_power, total_energy, total_readings, total_anomalies)
VALUES (1, 0.0, 0.0, 0.0, 0.0, 0, 0)
ON DUPLICATE KEY UPDATE id = 1;
