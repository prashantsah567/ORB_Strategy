'''
This file only need to run once in the beginning or if new dataset are added in historical_data folder.
It process all the data in the historical_data folder and store in Processed_data folder 
'''

import os
import pandas as pd # type: ignore
import numpy as np # type: ignore

RAW_DATA_FOLDER = './historical_data_new'
PROCESSED_DATA_FOLDER = './processed_data_new'

#calculate ATR, Average Volume & Relative Volume
def calculate_indicators(df):
    df['high_low'] = df['high'] - df['low']
    df['high_close'] = np.abs(df['high'] - df['close'].shift())
    df['low_close'] = np.abs(df['low'] - df['close'].shift())
    df['true_range'] = df[['high_low', 'high_close', 'low_close']].max(axis=1)
    df['ATR_14'] = df['true_range'].rolling(window=14).mean()
    df['Avg_Volume_14d'] = df['volume'].rolling(window=14*390).mean()
    df['Relative_Volume'] = df['volume'] / df['Avg_Volume_14d']

    return df

def preprocess_historical_data():
    if not os.path.exists(PROCESSED_DATA_FOLDER):
        os.makedirs(PROCESSED_DATA_FOLDER)

    for file in os.listdir(RAW_DATA_FOLDER):
        if file.endswith('_1_min_data.csv'):
            ticker = file.replace('_1_min_data.csv', '')
            raw_file_path = os.path.join(RAW_DATA_FOLDER, file)
            processed_file_path = os.path.join(PROCESSED_DATA_FOLDER, f"{ticker}.parquet")

            try:
                # Load raw CSV
                df = pd.read_csv(raw_file_path, parse_dates=['timestamp'])

                # Convert 'timestamp' to timezone-aware and set as index
                df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True).dt.tz_convert('US/Eastern')
                df.set_index('timestamp', inplace=True)

                # Filter trading hours
                trading_hours = df.between_time('09:30', '16:00').copy()

                # Calculate ATR for trading hours and update Dataframe
                trading_hours = calculate_indicators(trading_hours)

                # Save in Parquet format
                trading_hours.to_parquet(processed_file_path)
                print(f"Processed and saved: {ticker}")
            except Exception as e:
                print(f"Error processing {ticker}: {e}")

preprocess_historical_data()
