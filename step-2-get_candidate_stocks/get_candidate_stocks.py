'''Process all the data in the processed_data folder to get the top trading stocks on each day based on defined condition at !1'''

import os
import pandas as pd # type: ignore
from datetime import datetime
import pytz # type: ignore

#!1 - constants
MIN_OPEN_PRICE = 5.0
MIN_AVG_VOLUME = 10000 #lowering the volume (from 1_000_000) to see more result
MIN_ATR = 0.5
MIN_RELATIVE_VOLUME = 2.0
TOP_STOCKS_COUNT = 20 #max 20 stocks for a certain trading day

#folders and trading hours
data_folder = './processed_data_new'
start_time = '09:30:00'
end_time = '09:35:00' #putting end time as 9:35, so that we only choose those stocks which fits our criteria in the first 5 mins
start_date = '2022-11-30' # date that fits all dataset after calculating all the indicatros ATR, 14_day_avg and Relative Volume
end_date = '2024-11-27'

#load and filter data
def load_filtered_data(file_path):
    #df = pd.read_csv(file_path, parse_dates=['timestamp']) #this format will work for .csv not for .parquet

    #for .parquet file and since 'timestamp' is a index and not a regular columns
    df = pd.read_parquet(file_path)
    df['timestamp'] = df.index

    #convert start and end timestamp with date and time to timestamp object
    # start_timestamp = pd.Timestamp(f"{start_date} 09:30:00-05:00")
    # end_timestamp = pd.Timestamp(f"{end_date} 09:35:00-05:00") 
    
    # Define start and end timestamps dynamically in 'America/New_York'
    eastern = pytz.timezone('America/New_York')
    start_timestamp = eastern.localize(pd.Timestamp(f"{start_date} 09:30:00"))
    end_timestamp = eastern.localize(pd.Timestamp(f"{end_date} 09:35:00"))

    #filter by date and time
    df = df[(df['timestamp'] >= start_timestamp) & (df['timestamp'] <= end_timestamp)]

    #ensure 'timestamp' is in datetime format without the last UTC part (-5:00)
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True).dt.tz_convert('America/New_York').dt.tz_localize(None)
    df = df[df['timestamp'].dt.time.between(datetime.strptime(start_time, '%H:%M:%S').time(), datetime.strptime(end_time, '%H:%M:%S').time())]

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
        if filename.endswith(".parquet"):
            ticker = filename.split(".parquet")[0] 
            file_path = os.path.join(data_folder, filename)

            df = load_filtered_data(file_path)
            print(f"\nAfter loading data--------------{df.shape}-------------------and head:\n")
            #print(df.head)

            daily_stocks = select_top_stocks(df, ticker)
            print(f"\nAfter applying filter and selecting top stocks-------{daily_stocks.shape}---------------\n")
            #print(daily_stocks.head)

            all_stocks = pd.concat([all_stocks, daily_stocks], ignore_index=True)

    #top 20 stocks each day based on relative volume
    top_daily_stocks = all_stocks.sort_values(by=['date', 'Relative_Volume'], ascending=[True, False]).groupby('date') \
                       .head(TOP_STOCKS_COUNT).reset_index(drop=True)
    
    #without 20 limit
    #top_daily_stocks = all_stocks.sort_values(by=['date', 'Relative_Volume'], ascending=[True, False]).reset_index(drop=True)
    
    return top_daily_stocks

#run the top stocks finder and save results
top_stocks = find_top_stocks(data_folder)
print(f"The final result of top stocks------------------------{top_stocks.shape}---------------------------------")

if top_stocks.shape[0] > 0:
    top_stocks.to_csv('step-2-get_candidate_stocks/top_qualified_daily_stocks_20_max.csv', index=False)
else:
    print("No data available for the selected criteria.")
