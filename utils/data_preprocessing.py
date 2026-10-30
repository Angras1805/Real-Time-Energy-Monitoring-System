import sys
import os
import json
import logging
import zipfile
import urllib.request
from pathlib import Path
from typing import Dict, Any, Optional, Generator

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import numpy as np

from config.settings import DATA_CSV_PATH, DATA_DIR

logger = logging.getLogger("DataPreprocessing")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

UCI_URL = "https://archive.ics.uci.edu/static/public/235/individual+household+electric+power+consumption.zip"


def generate_benchmark_dataset(output_path: Path, n_records: int = 15000) -> Path:
    """
    Generates a realistic smart meter dataset simulating household power consumption
    with daily cycles, weekend variances, random appliance usage, and occasional anomalies.
    """
    logger.info(f"Generating realistic benchmark energy dataset with {n_records} readings...")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(seed=42)
    start_time = pd.Timestamp("2026-01-01 00:00:00")
    timestamps = [start_time + pd.Timedelta(minutes=i) for i in range(n_records)]

    hours = np.array([ts.hour for ts in timestamps])
    day_of_week = np.array([ts.dayofweek for ts in timestamps])

    # Diurnal cycle: low at night (0.3-0.8 kW), morning peak (1.8-3.5 kW), evening peak (2.2-4.5 kW)
    base_cycle = 0.5 + 1.2 * np.sin(np.pi * (hours - 4) / 12) ** 2
    evening_boost = 1.4 * np.exp(-((hours - 20) ** 2) / 8)
    morning_boost = 1.1 * np.exp(-((hours - 8) ** 2) / 6)
    weekend_factor = np.where(day_of_week >= 5, 1.25, 1.0)

    noise = rng.normal(0, 0.15, n_records)
    active_power = (base_cycle + morning_boost + evening_boost) * weekend_factor + noise
    active_power = np.clip(active_power, 0.15, 6.0)

    # Inject realistic anomalies (~3-4% of points)
    # Types: extreme surge (heavy equipment/fault: 7-10 kW), sudden zero drop (outage/sensor fault: 0.0 kW)
    anomaly_mask = rng.random(n_records) < 0.035
    for idx in np.where(anomaly_mask)[0]:
        if rng.random() < 0.65:
            # Power spike / overload
            active_power[idx] = rng.uniform(7.2, 10.5)
        else:
            # Sudden blackout / drop / zero consumption
            active_power[idx] = rng.uniform(0.0, 0.05)

    # Derive voltage and reactive power
    voltage = 230.0 + rng.normal(0, 3.5, n_records) - (active_power * 1.5)
    voltage = np.clip(voltage, 210.0, 250.0)

    reactive_power = active_power * rng.uniform(0.08, 0.25, n_records)
    current_intensity = (active_power * 1000) / voltage

    df = pd.DataFrame({
        "reading_time": [ts.strftime("%Y-%m-%d %H:%M:%S") for ts in timestamps],
        "global_active_power": np.round(active_power, 3),
        "global_reactive_power": np.round(reactive_power, 3),
        "voltage": np.round(voltage, 2),
        "global_intensity": np.round(current_intensity, 2),
        "sub_metering_1": np.round(np.where(hours.isin([7, 8, 19, 20]), rng.uniform(0, 15, n_records), 0), 1),
        "sub_metering_2": np.round(np.where(hours.isin([9, 10, 18, 19]), rng.uniform(0, 10, n_records), 0), 1),
        "sub_metering_3": np.round(active_power * rng.uniform(10, 18, n_records), 1),
    })

    df.to_csv(output_path, index=False)
    logger.info(f"Successfully generated dataset at {output_path} ({len(df)} records).")
    return output_path


def load_or_create_dataset(target_csv: Optional[Path] = None, max_rows: int = 30000) -> Path:
    """
    Ensures smart_meter.csv exists. Attempts to fetch or download the UCI dataset,
    and falls back to high-fidelity benchmark generation if necessary.
    """
    csv_file = target_csv or DATA_CSV_PATH
    if csv_file.exists() and csv_file.stat().st_size > 1024:
        logger.info(f"Dataset already exists at {csv_file}")
        return csv_file

    csv_file.parent.mkdir(parents=True, exist_ok=True)
    zip_path = csv_file.parent / "uci_power.zip"

    try:
        logger.info("Attempting to download UCI Household Electric Power Consumption dataset...")
        req = urllib.request.Request(UCI_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as response, open(zip_path, "wb") as out_file:
            out_file.write(response.read())

        with zipfile.ZipFile(zip_path, "r") as z:
            txt_files = [f for f in z.namelist() if f.endswith(".txt")]
            if txt_files:
                z.extract(txt_files[0], csv_file.parent)
                extracted_txt = csv_file.parent / txt_files[0]
                logger.info(f"Extracted {extracted_txt}. Cleaning and converting to CSV...")
                
                # Load first max_rows from UCI dataset
                df = pd.read_csv(
                    extracted_txt,
                    sep=";",
                    na_values=["?"],
                    nrows=max_rows,
                    low_memory=False,
                )
                
                # Combine Date and Time
                df["reading_time"] = pd.to_datetime(
                    df["Date"] + " " + df["Time"],
                    format="%d/%m/%Y %H:%M:%S",
                    errors="coerce",
                ).dt.strftime("%Y-%m-%d %H:%M:%S")

                # Rename columns to standard names
                col_rename = {
                    "Global_active_power": "global_active_power",
                    "Global_reactive_power": "global_reactive_power",
                    "Voltage": "voltage",
                    "Global_intensity": "global_intensity",
                    "Sub_metering_1": "sub_metering_1",
                    "Sub_metering_2": "sub_metering_2",
                    "Sub_metering_3": "sub_metering_3",
                }
                df = df.rename(columns=col_rename)

                # Keep required columns and clean NaNs
                selected_cols = ["reading_time", "global_active_power", "global_reactive_power", "voltage", "global_intensity", "sub_metering_1", "sub_metering_2", "sub_metering_3"]
                df = df[[c for c in selected_cols if c in df.columns]]
                df = df.dropna().reset_index(drop=True)
                
                df.to_csv(csv_file, index=False)
                logger.info(f"Saved {len(df)} cleaned UCI records to {csv_file}")
                
                # Clean up temporary raw txt and zip
                try:
                    extracted_txt.unlink()
                    zip_path.unlink()
                except Exception:
                    pass
                return csv_file
    except Exception as err:
        logger.warning(f"Could not download or parse UCI dataset directly ({err}). Using realistic benchmark generator.")

    # Fallback to realistic benchmark dataset
    return generate_benchmark_dataset(csv_file, n_records=max_rows)


def clean_record(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Validates, sanitizes, and type-checks an incoming raw reading row.
    Returns sanitized dictionary or None if row is malformed.
    """
    try:
        reading_time = str(row.get("reading_time", "")).strip()
        if not reading_time:
            return None

        power = float(row.get("global_active_power", -1))
        if power < 0 or np.isnan(power):
            return None

        voltage = float(row.get("voltage", 230.0))
        reactive = float(row.get("global_reactive_power", 0.0))
        intensity = float(row.get("global_intensity", 0.0))

        return {
            "reading_time": reading_time,
            "global_active_power": round(power, 4),
            "global_reactive_power": round(reactive, 4),
            "voltage": round(voltage, 2),
            "global_intensity": round(intensity, 2),
            "sub_metering_1": float(row.get("sub_metering_1", 0.0)),
            "sub_metering_2": float(row.get("sub_metering_2", 0.0)),
            "sub_metering_3": float(row.get("sub_metering_3", 0.0)),
        }
    except (ValueError, TypeError) as err:
        logger.debug(f"Invalid record rejected: {row} (Error: {err})")
        return None


def stream_csv_rows(csv_path: Path) -> Generator[Dict[str, Any], None, None]:
    """
    Generator that reads the dataset row by row and yields sanitized event dictionaries.
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"Dataset not found at {csv_path}")

    # Read CSV in chunks for memory-efficiency
    chunksize = 1000
    for chunk in pd.read_csv(csv_path, chunksize=chunksize):
        for _, raw_row in chunk.iterrows():
            cleaned = clean_record(raw_row.to_dict())
            if cleaned:
                yield cleaned


if __name__ == "__main__":
    load_or_create_dataset()
