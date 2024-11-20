import pandas as pd # type: ignore
import os

'''
start_date = '2022-11-28'
end_date = '2024-11-05'

# Convert start and end timestamps with date and time to Timestamp objects
start_timestamp = pd.Timestamp(f"{start_date} 09:30:00-05:00")
end_timestamp = pd.Timestamp(f"{end_date} 15:59:00-05:00")
print(start_timestamp)
print("\n", end_timestamp)

if start_timestamp < end_timestamp:
    print("All good")
# Filter df based on the converted Timestamp objects
#df = df[(df['timestamp'] >= start_timestamp) & (df['timestamp'] <= end_timestamp)]
'''

# #to check how many times a value is repeated
# df = pd.read_csv('top_daily_stocks.csv')
# tickers_count = df['ticker'].value_counts()

# print(tickers_count)
# print(f'\n{tickers_count.sum()}')

#raw_file_path = os.path.join('historical_data', 'TSLA_1_min_data.csv')
# processed_file_path = os.path.join('processed_data', "TSLA.parquet")

# try:
#     # Load raw CSV and preprocess
#     df = pd.read_csv('historical_data/TSLA_1_min_data.csv', parse_dates=['timestamp'])
#     df.set_index('timestamp', inplace=True)

#     # Save in Parquet format
#     df.to_parquet(processed_file_path)
#     print(f"Processed and saved: TSLA")
# except Exception as e:
#     print(f"Error processing TSLA file: {e}")


def convert_parquet_to_csv(parquet_file, csv_file):

    # Read the Parquet file into a pandas DataFrame
    df = pd.read_parquet(parquet_file)

    # Write the DataFrame to a CSV file
    df.to_csv(csv_file)

# Example usage:
parquet_file = 'processed_data/ADSK.parquet'
csv_file = 'new_ADSK.csv'

convert_parquet_to_csv(parquet_file, csv_file)