import sys
import logging
from pathlib import Path

# Add project root to sys.path so modules can be imported when running script directly
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
import joblib

from config.settings import (
    DATA_CSV_PATH,
    MODEL_PATH,
    MODEL_DIR,
    CONTAMINATION_RATE,
    RANDOM_STATE,
    SLIDING_WINDOW_SIZE,
)
from utils.data_preprocessing import load_or_create_dataset

logger = logging.getLogger("TrainModel")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def train_isolation_forest(
    data_path: Path = DATA_CSV_PATH,
    model_output_path: Path = MODEL_PATH,
    contamination: float = CONTAMINATION_RATE,
    random_state: int = RANDOM_STATE,
    window_size: int = SLIDING_WINDOW_SIZE,
) -> IsolationForest:
    """
    Loads historical smart meter data, computes sliding window rolling features,
    trains an Isolation Forest model to detect energy anomalies, and saves the artifact.
    """
    logger.info("=== Starting Isolation Forest Model Training ===")

    # 1. Ensure dataset exists
    if not data_path.exists():
        logger.info(f"Dataset not found at {data_path}. Loading/generating data...")
        load_or_create_dataset(data_path)

    # 2. Load dataset
    logger.info(f"Loading historical electricity data from {data_path}...")
    df = pd.read_csv(data_path)
    logger.info(f"Loaded {len(df):,} records from dataset.")

    # 3. Preprocess and engineer rolling features
    # Ensure sorted chronologically
    df["reading_time"] = pd.to_datetime(df["reading_time"])
    df = df.sort_values("reading_time").reset_index(drop=True)

    # Compute rolling average for feature alignment with real-time stream consumer
    df["rolling_avg_30"] = (
        df["global_active_power"]
        .rolling(window=window_size, min_periods=1)
        .mean()
    )

    feature_cols = ["global_active_power", "rolling_avg_30"]
    X = df[feature_cols].copy()

    # Drop any unexpected NaNs
    X = X.dropna()

    logger.info(f"Training features: {feature_cols}")
    logger.info(f"Feature matrix shape: {X.shape}")
    logger.info(f"Hyperparameters: contamination={contamination}, n_estimators=100, random_state={random_state}")

    # 4. Train Isolation Forest
    model = IsolationForest(
        n_estimators=100,
        contamination=contamination,
        random_state=random_state,
        n_jobs=-1,
    )

    model.fit(X)

    # 5. Evaluate and inspect predictions
    preds = model.predict(X)  # 1 for normal, -1 for anomaly
    decision_scores = model.decision_function(X)

    df["prediction"] = preds
    df["is_anomaly"] = np.where(preds == -1, 1, 0)
    df["anomaly_score"] = decision_scores

    n_total = len(df)
    n_anomalies = (df["is_anomaly"] == 1).sum()
    n_normal = n_total - n_anomalies
    pct_anomaly = (n_anomalies / n_total) * 100

    logger.info("=== Model Evaluation & Anomaly Summary ===")
    logger.info(f"Total Records Evaluated : {n_total:,}")
    logger.info(f"Normal Readings (1)     : {n_normal:,} ({100 - pct_anomaly:.2f}%)")
    logger.info(f"Anomalous Readings (-1) : {n_anomalies:,} ({pct_anomaly:.2f}%)")

    normal_power = df[df["is_anomaly"] == 0]["global_active_power"]
    anomaly_power = df[df["is_anomaly"] == 1]["global_active_power"]

    logger.info(
        f"Normal Power Range     : {normal_power.min():.2f} kW - {normal_power.max():.2f} kW (Mean: {normal_power.mean():.2f} kW)"
    )
    logger.info(
        f"Anomalous Power Range  : {anomaly_power.min():.2f} kW - {anomaly_power.max():.2f} kW (Mean: {anomaly_power.mean():.2f} kW)"
    )

    # 6. Save model artifact
    model_output_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(model, model_output_path)
    logger.info(f"Model saved successfully as: {model_output_path}")

    return model


if __name__ == "__main__":
    train_isolation_forest()
