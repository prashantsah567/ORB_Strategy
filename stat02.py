import os
import pandas as pd # type: ignore
from datetime import datetime

#constants
MIN_OPEN_PRICE = 5.0
MIN_AVG_VOLUME = 5000 #lowering the volume (from 1_000_000) to see more result
MIN_ATR = 0.20 #lowering the ATR (from 0.5) to see more results
MIN_RELATIVE_VOLUME = 2.0
TOP_STOCKS_COUNT = 20

#folders and trading hours
data_folder = 'historical_data'
start_time = '09:30:00'
end_time = '15:59:00'
start_date = '2022-11-09'
end_date = '2024-11-05'

#load and filter data
def load_filtered_data(file_path):
    df = pd.read_csv(file_path, parse_dates=['timestamp'])

    #convert start and end timestamp with date and time to timestamp object
    start_timestamp = pd.Timestamp(f"{start_date} 09:30:00-05:00")
    end_timestamp = pd.Timestamp(f"{end_date} 15:59:00-05:00")
    #filter by date and time
    df = df[(df['timestamp'] >= start_timestamp) & (df['timestamp'] <= end_timestamp)]

    #ensure 'timestamp' is in datetime format without the last UTC part (-5:00)
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True).dt.tz_convert('America/New_York').dt.tz_localize(None)
    df = df[df['timestamp'].dt.time.between(datetime.strptime(start_time, '%H:%M:%S').time(), datetime.strptime(end_time, '%H:%M:%S').time())]

    return df

#calculate ATR(14-day), Average Volume(14-day), and Relative Volume
def calculate_indicators(df):
    df['TR'] = abs(df['high'] - df['low'])
    df['ATR_14'] = df['TR'].rolling(window=14*390).mean() #390 minute in a trading day

    df['Avg_Volume_14d'] = df['volume'].rolling(window=14*390).mean()

    df['Relative_Volume'] = df['volume'] / df['Avg_Volume_14d']

    return df

#select top stocks (based on our criteria)
def select_top_stocks(df,ticker):
    df = df[(df['open'] >= MIN_OPEN_PRICE) & (df['Avg_Volume_14d'] >= MIN_AVG_VOLUME)
            & (df['ATR_14'] >= MIN_ATR) & (df['Relative_Volume'] >= MIN_RELATIVE_VOLUME)]

    #select one entry per day meeting the criteria (the first entry of each stock each day to later filter out the top 20 stocks)
    df['date'] = df['timestamp'].dt.date
    daily_stocks = df.groupby('date').apply(lambda x: x.head(1)).reset_index(drop=True)
    daily_stocks['ticker'] = ticker

    return daily_stocks

#process all tickers and find top stocks per day
def find_top_stocks(data_folder):
    all_stocks = pd.DataFrame()

    for filename in os.listdir(data_folder):
        print(f"Processing file----------------------------------------------->: {filename}")
        if filename.endswith("_1_min_data.csv"):
            ticker = filename.split("_")[0]
            file_path = os.path.join(data_folder, filename)

            df = load_filtered_data(file_path)
            print(f"\nAfter loading data--------------{df.shape}-------------------and head:\n")
            print(df.head)

            df = calculate_indicators(df)
            print(f"\nAfter Calculating indicators--------------{df.shape}---------and head:\n")
            print(df.head)

            daily_stocks = select_top_stocks(df, ticker)
            print(f"\nAfter applying filter and selecting top stocks-------{daily_stocks.shape}\n")
            print(daily_stocks.head)

            all_stocks = pd.concat([all_stocks, daily_stocks], ignore_index=True)

    #find top 20 stocks each day based on relative volume (**might change*)
    top_daily_stocks = all_stocks.sort_values(by=['date', 'Relative_Volume'], ascending=[True, False]).groupby('date') \
                       .head(TOP_STOCKS_COUNT).reset_index(drop=True)
    #if don't want to put 20 limit
    #top_daily_stocks = all_stocks.sort_values(by=['date', 'Relative_Volume'], ascending=[True, False]).reset_index(drop=True)
    
    return top_daily_stocks

#run the top stocks finder and save results
top_stocks = find_top_stocks(data_folder)
print(f"The final result of top stocks------------------------{top_stocks.shape}---------------------------------")

if top_stocks.shape[0] > 0:
    top_stocks.to_csv('top_daily_stocks.csv', index=False)
else:
    print("No data available for the selected criteria.")
