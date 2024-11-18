import os
import pandas as pd

RAW_DATA_FOLDER = 'historical_data'
PROCESSED_DATA_FOLDER = 'processed_data'

def preprocess_historical_data():
    if not os.path.exists(PROCESSED_DATA_FOLDER):
        os.makedirs(PROCESSED_DATA_FOLDER)

    for file in os.listdir(RAW_DATA_FOLDER):
        if file.endswith('_1_min_data.csv'):
            ticker = file.replace('_1_min_data.csv', '')
            raw_file_path = os.path.join(RAW_DATA_FOLDER, file)
            processed_file_path = os.path.join(PROCESSED_DATA_FOLDER, f"{ticker}.parquet")

            try:
                # Load raw CSV and preprocess
                df = pd.read_csv(raw_file_path, parse_dates=['timestamp'])
                df.set_index('timestamp', inplace=True)

                # Save in Parquet format
                df.to_parquet(processed_file_path)
                print(f"Processed and saved: {ticker}")
            except Exception as e:
                print(f"Error processing {ticker}: {e}")

preprocess_historical_data()
