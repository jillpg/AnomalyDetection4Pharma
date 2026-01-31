
import os
import sys
import argparse
import numpy as np

sys.path.append(os.path.abspath('src'))
from config import get_spark_session, get_data_path, ALL_SENSORS
from models.baselines import BaselineModels

WINDOW_SIZE = 60
BATCH_SIZE = 64
EPOCHS = 50       # Production setting
PATIENCE = 15
MODELS_DIR = "models/production"
DATA_DIR = "gold"


def create_sliding_windows(pdf, window_size, sensor_cols, batch_col="batch"):
    """
    Creates moving windows respecting batch boundaries.
    """
    windows = []

    # Group by batch
    grouped = pdf.groupby(batch_col)

    count = 0
    for _, group in grouped:
        # Sort by timestamp
        group = group.sort_values("timestamp")
        data = group[sensor_cols].values

        n_samples = len(data)
        if n_samples < window_size:
            continue

        # Create strided windows
        # Shape: (N - W + 1, W, F)
        num_windows = n_samples - window_size + 1
        sub_windows = np.lib.stride_tricks.sliding_window_view(data, window_shape=(window_size, len(sensor_cols)))
        # Reshape to ensure (N, W, F) structure
        sub_windows = sub_windows.reshape(num_windows, window_size, len(sensor_cols))

        windows.append(sub_windows)
        count += 1

    print(f"   Processed {count} batches into windows.")
    return np.concatenate(windows, axis=0)


def load_data(spark):
    """
    Loads Gold data from Parquet and converts strict features to Pandas.
    """
    gold_path = get_data_path(DATA_DIR)
    print(f"\n📦 Loading Gold Data from: {gold_path}")

    try:
        df_train = spark.read.parquet(f"{gold_path}/train.parquet")
        df_val = spark.read.parquet(f"{gold_path}/val.parquet")
    except Exception as e:
        print(f"❌ Error loading data from {gold_path}: {e}")
        sys.exit(1)

    print(f"   Features: {ALL_SENSORS}")

    # Convert to Pandas
    pdf_train = df_train.select("batch", "timestamp", *ALL_SENSORS).toPandas()
    pdf_val = df_val.select("batch", "timestamp", *ALL_SENSORS).toPandas()

    print(f"   Train Rows: {len(pdf_train)}")
    print(f"   Val Rows:   {len(pdf_val)}")

    return pdf_train, pdf_val


def train_baselines(pdf_train):
    """
    Trains Isolation Forest and PCA models.
    """
    print("\n🌲 [1/2] Training Baseline Models...")

    X_train_base = pdf_train[ALL_SENSORS]

    # A) Isolation Forest
    print("   -> Isolation Forest (n=100, cont=0.05)...")
    iso_forest = BaselineModels("isolation_forest", n_estimators=100, contamination=0.05, random_state=42, n_jobs=-1)
    iso_forest.train(X_train_base)
    iso_forest.save(f"{MODELS_DIR}/isolation_forest.pkl")

    # B) PCA
    print("   -> PCA (n_components=3)...")
    pca = BaselineModels("pca", n_components=3)
    pca.train(X_train_base)
    pca.save(f"{MODELS_DIR}/pca.pkl")


def train_lstm(pdf_train, pdf_val):
    """
    Trains LSTM Autoencoder model.
    """
    # Lazy import to avoid forcing torch installation
    from models.lstm_ae import LSTMAutoencoder

    print("\n🧠 [2/2] Training LSTM Autoencoder...")

    print(f"   Creating Windows (Size={WINDOW_SIZE})...")
    X_train_lstm = create_sliding_windows(pdf_train, WINDOW_SIZE, ALL_SENSORS)
    X_val_lstm = create_sliding_windows(pdf_val, WINDOW_SIZE, ALL_SENSORS)

    print(f"   LSTM Input Shape: {X_train_lstm.shape}")

    # Initialize Model
    model = LSTMAutoencoder(window_size=WINDOW_SIZE, n_features=len(ALL_SENSORS))

    # Train
    model.train_model(
        X_train_lstm,
        X_val_lstm,
        epochs=EPOCHS,
        batch_size=BATCH_SIZE,
        patience=PATIENCE,
        save_path=f"{MODELS_DIR}/lstm_ae.pth"
    )

    print(f"   ✅ Saved to: {MODELS_DIR}/lstm_ae.pth")


def main():
    parser = argparse.ArgumentParser(description="Unified Training Pipeline")
    parser.add_argument("--models", type=str, default="all", choices=["all", "baseline", "lstm"],
                        help="Choose which models to train.")

    args = parser.parse_args()

    print("🚀 Starting Unified Training Pipeline...")
    print(f"🎯 Target Models: {args.models.upper()}")

    os.makedirs(MODELS_DIR, exist_ok=True)

    spark = get_spark_session("Train_Pipeline")

    # Load Data
    pdf_train, pdf_val = load_data(spark)

    # 1. Baselines
    if args.models in ["all", "baseline"]:
        train_baselines(pdf_train)
    else:
        print("\n⏩ Skipping Baseline Models.")

    # 2. LSTM
    if args.models in ["all", "lstm"]:
        train_lstm(pdf_train, pdf_val)
    else:
        print("\n⏩ Skipping LSTM Autoencoder.")

    print("\n✅ Pipeline Finished.")
    spark.stop()


if __name__ == "__main__":
    main()
