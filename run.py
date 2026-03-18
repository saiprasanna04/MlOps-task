import argparse
import yaml
import pandas as pd
import numpy as np
import logging
import time
import json
import sys
import os


# ------------------
# Logging Setup
# ------------------
def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


# ------------------
# Write Metrics
# ------------------
def write_metrics(output_path, data):
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)
    print(json.dumps(data, indent=2))  # stdout (required for Docker)


# ------------------
# Validate Config
# ------------------
def validate_config(config):
    required = ["seed", "window", "version"]
    for key in required:
        if key not in config:
            raise ValueError(f"Missing config field: {key}")


# ------------------
# Load Data (Robust CSV Fix)
# ------------------
def load_data(input_path):
    if not os.path.exists(input_path):
        raise FileNotFoundError("Input file not found")

    # First attempt (normal CSV)
    df = pd.read_csv(input_path)

    # Fix: entire row as single column
    if len(df.columns) == 1:
        logging.info("Malformed CSV detected. Applying fix.")

        df = pd.read_csv(input_path, header=None)
        df = df.iloc[:, 0].astype(str).str.split(",", expand=True)

        df.columns = [
            "timestamp", "open", "high", "low",
            "close", "volume_btc", "volume_usd"
        ]

    # Normalize columns
    df.columns = df.columns.str.strip().str.lower()

    logging.info(f"Columns detected: {list(df.columns)}")

    if df.empty:
        raise ValueError("Input CSV is empty")

    if "close" not in df.columns:
        raise ValueError("Missing 'close' column")

    return df


# ------------------
# Main Pipeline
# ------------------
def main(args):
    start_time = time.time()
    setup_logging(args.log_file)

    success_written = False

    try:
        logging.info("Job started")

        # ------------------
        # Load Config
        # ------------------
        if not os.path.exists(args.config):
            raise FileNotFoundError("Config file not found")

        with open(args.config, "r") as f:
            config = yaml.safe_load(f)

        validate_config(config)

        seed = config["seed"]
        window = config["window"]
        version = config["version"]

        np.random.seed(seed)

        logging.info(f"Config loaded: seed={seed}, window={window}, version={version}")

        # ------------------
        # Load Data
        # ------------------
        df = load_data(args.input)

        # Convert close safely
        df["close"] = pd.to_numeric(df["close"], errors="coerce")

        # Drop invalid values
        df = df.dropna(subset=["close"])

        # 🔥 IMPORTANT: limit to exactly 10000 rows (assignment requirement)
        df = df.head(10000)

        logging.info(f"Rows after cleaning: {len(df)}")

        # ------------------
        # Processing
        # ------------------
        logging.info("Computing rolling mean")
        df["rolling_mean"] = df["close"].rolling(window=window).mean()

        logging.info("Generating signals")
        df["signal"] = np.where(df["close"] > df["rolling_mean"], 1, 0)

        # Remove NaN rows from rolling
        valid_df = df.dropna(subset=["rolling_mean"])

        if len(valid_df) == 0:
            raise ValueError("No valid rows after rolling computation")

        signal_rate = valid_df["signal"].mean()

        rows_processed = len(df)  # should now be 10000

        latency_ms = int((time.time() - start_time) * 1000)

        metrics = {
            "version": version,
            "rows_processed": rows_processed,
            "metric": "signal_rate",
            "value": round(float(signal_rate), 4),
            "latency_ms": latency_ms,
            "seed": seed,
            "status": "success"
        }

        # ✅ WRITE FIRST (critical fix)
        write_metrics(args.output, metrics)
        success_written = True

        logging.info(f"Metrics: {metrics}")
        logging.info("Job completed successfully")

        sys.exit(0)

    except Exception as e:
        logging.error(f"Error: {str(e)}")

        error_output = {
            "version": "v1",
            "status": "error",
            "error_message": str(e)
        }

        # ✅ only write error if success not written
        if not success_written:
            write_metrics(args.output, error_output)

        sys.exit(1)


# ------------------
# CLI Entry
# ------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--log-file", required=True)

    args = parser.parse_args()

    main(args)
