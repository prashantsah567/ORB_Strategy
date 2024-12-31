'''This file has the core logic (buy/sell) that applies to the candidate stocks from step-2'''

import pandas as pd # type: ignore
from datetime import datetime
import os
import time
import pytz # type: ignore
import shutil

TOP_STOCKS_FILE = './step-2-get_candidate_stocks/top_20_qualified_daily_stocks.csv' #file from step-2
PROCESSED_DATA_FOLDER = './processed_data_new'
LOG_FILE = 'trade_log_initial.csv'
STOP_LOSS_PERCENTAGE = 0.05 #5% of atr
atr_value = 0.15 #fixed atr_value used for calculating stop loss (so net stop loss will be atr_value * STOP_LOSS_PERCENTAGE = 0.15 * 0.05 = 0.0075 which is 0.75%)
PERCENTAGE_CHANGE_BEFORE_ENTRY = 0.0025 #0.25% change before entry
#trail_percent = 0.02 # 2% (can be used for trailing stop loss, not used in this script because of lower return)

#helper function to get tickers for a given date
def get_tickers_for_date(date):
    df = pd.read_csv(TOP_STOCKS_FILE)
    daily_stocks = df[df['date'] == date]
    tickers = daily_stocks['ticker'].tolist()

    return tickers

# load historical data for selected tickers (for .csv) - if not willing to use parquet files
'''
def load_historical_data(tickers):
    data = {}
    for ticker in tickers:
        file_path = f'{HISTORICAL_DATA_FOLDER}/{ticker}_1_min_data.csv'
        try:
            df = pd.read_csv(file_path, parse_dates=['timestamp'])
            df.set_index('timestamp', inplace=True)
            data[ticker] = df
        except FileNotFoundError:
            print(f'Historical data not found for ticker: {ticker}')

    return data
'''

# loading function based on processed parquet files
def load_historical_data(tickers):
    data = {}
    for ticker in tickers:
        file_path = f'{PROCESSED_DATA_FOLDER}/{ticker}.parquet'
        try:
            # Load preprocessed Parquet file
            df = pd.read_parquet(file_path)
            data[ticker] = df
        except FileNotFoundError:
            print(f'Processed data not found for ticker: {ticker}')
        except Exception as e:
            print(f"Error loading data for {ticker}: {e}")

    return data

#check price movement between 09:30 and 09:35 and decide to go long or short
def check_price_movement(data, date):
    # Define the US/Eastern timezone using pytz
    eastern = pytz.timezone('US/Eastern')
    
    # Construct the start and end times dynamically based on the date
    start_time = pd.Timestamp(f"{date} 09:30:00").tz_localize(eastern, ambiguous='NaT')
    end_time = pd.Timestamp(f"{date} 09:35:00").tz_localize(eastern, ambiguous='NaT')
    
    try:
        data_filtered = data.loc[start_time:end_time]
    except KeyError as e:
        print(f"Missing data for the range {start_time} to {end_time}.") #for any missing timestamp
        return 'no_trade',0

    #count how many of the candles (from 09:30 to 09:35) had close > open
    positive_movement = sum(data_filtered['close'] > data_filtered['open'])

    #if more than 5 candles had close > open, go long; if less than 1, go short else no trade
    if positive_movement >= 5:
        lowest_ob_price = data_filtered[['open', 'close']].min().min()
        return 'long', lowest_ob_price
    elif positive_movement <= 1:
        highest_ob_price = data_filtered[['open', 'close']].max().max()
        return 'short', highest_ob_price
    else:
        return 'no_trade',0

#stop loss calculation
def calculate_stop_loss(entry_price, atr, position_type):
    if position_type == 'long':
        return entry_price - (entry_price * STOP_LOSS_PERCENTAGE * atr)
    elif position_type == 'short':
        return entry_price + (entry_price * STOP_LOSS_PERCENTAGE * atr)

#trailing stop loss calculation (uncomment this function to use trailing stop loss)
'''
def update_trailing_stop_loss(current_price, highest_price, atr, trail_percent, position_type):
    if position_type == 'long':
        #update trailing stop only if the current price exceeds the highest recorded price
        if current_price > highest_price:
            highest_price = current_price
        #re-calculate stop loss based on highest price
        trailing_stop = highest_price - (trail_percent * highest_price)
    elif position_type == 'short':
        #update trailing stop only if the current price falls below the lowest recorded price
        if current_price < highest_price:
            highest_price = current_price
        #re-calculate stop loss based on lowest price
        trailing_stop = highest_price + (trail_percent * highest_price)

    return trailing_stop, highest_price
'''

#log trades
def log_trade(action, ticker, price, entry_time, position_type):
    if not os.path.exists('logs'):
        os.makedirs('logs')

    log_file_path = f'logs/{LOG_FILE}'

    log_entry = {
        'status': action,
        'ticker': ticker,
        'price': price,
        'timestamp': entry_time,
        'position_type': position_type
    }

    write_header = not os.path.exists(log_file_path) #true only if file doesn't exists
    log_df = pd.DataFrame([log_entry])
    log_df.to_csv(log_file_path, mode='a', header=write_header, index=False)

# main logic for a given trading day
def process_trading_day(date):
    #get tickers for the selected day
    tickers = get_tickers_for_date(date)
    print(f'Tickers for {date}: {tickers}')

    #load historical data for tickers on the selected day
    historical_data = load_historical_data(tickers)

    positions = [] #to store open positions for the day

    #check price movement for each ticker
    for ticker, data in historical_data.items():
        print(f"Analyzing {ticker}...")
        position, ob_price = check_price_movement(data, date)

        if position != 'no_trade':
            # Define the US/Eastern timezone using pytz
            eastern = pytz.timezone('US/Eastern')
            # Construct the start and end times dynamically based on the date
            start_time = pd.Timestamp(f"{date} 09:35:00").tz_localize(eastern, ambiguous='NaT')

            #checking for half day
            half_days = ['2022-07-03', '2023-07-03', '2023-11-24', '2024-07-03']

            #end time is fixed for all days (full or half), unless it hit the stop loss
            if date in half_days:
                end_time = pd.Timestamp(f"{date} 12:55:00").tz_localize(eastern, ambiguous='NaT')
            else:
                end_time = pd.Timestamp(f"{date} 15:55:00").tz_localize(eastern, ambiguous='NaT')

            #filtering the trading time data for a certain day (1-minute interval - intraday)
            entry_data = data[(data.index >= start_time) & (data.index <= end_time)]

            if entry_data.empty:
                continue

            #this is the time & price for the lookout of entry
            entry_time_init = entry_data.index[0]
            entry_price_init = data.loc[entry_time_init,'close']

            entry_time = None
            entry_price = None

            #loop through the data to find the entry time that satisfy PERCENTAGE_CHANGE_BEFORE_ENTRY
            for timestamp, row in entry_data.iterrows():
                current_price = row['close']

                if position == 'long' and current_price <= (entry_price_init - (entry_price_init * PERCENTAGE_CHANGE_BEFORE_ENTRY)):
                    entry_time = timestamp
                    entry_price = current_price
                    break #got the entry, exit out of loop
                elif position == 'short' and current_price >= (entry_price_init + (entry_price_init * PERCENTAGE_CHANGE_BEFORE_ENTRY)):
                    entry_time = timestamp
                    entry_price = current_price
                    break #got the entry, exit out of loop

            '''this part of code is to put a limit buy (either lowest or highest in the first 5 mins)'''
            '''
            entry_time = None
            entry_price = None

            #entering the trade only when we get the price (low or high in the first 5 minutes) -- (limit buy)
            for timestamp, row in entry_data.iterrows():
                current_price = row['close']

                if position == 'long' and current_price <= ob_price:
                    entry_time = timestamp
                    entry_price = current_price
                    break
                elif position == 'short' and current_price >= ob_price:
                    entry_time = timestamp
                    entry_price = current_price
                    break

            #skip to the next ticker if no trade entry occured
            if entry_time is None or entry_price is None:
                continue
            '''

            #incase there are multiple values at the entry_price, choose the first one
            if isinstance (entry_price, pd.Series):
                entry_price = entry_price.iloc[0] #handle ambiguity
            
            '''if we want to use ATR (from processed data file) for stop loss calculation'''
            # atr_value = data.loc[entry_time, 'ATR_14']
            # if atr_value > 0.3: #max_atr value is 0.3 which would limit max stop loss to 3%
            #     atr_value = 0.3

            #only go for any calculation or exit position if we entered a position
            if entry_price is not None:
                
                #for the exit time, the timestamp need to start from the entry time
                entry_data = data[(data.index > entry_time) & (data.index <= end_time)] #start_time is entry_time now

                #calculate stop loss for long or short
                stop_loss = calculate_stop_loss(entry_price, atr_value, position)

                #for trailing stop loss (for long it's the highest price and for short it's the lowest price)
                #highest_price = entry_price #+ (entry_price * 0.015) 

                #log the opening of the trade
                log_trade('open', ticker, entry_price, entry_time, position)
                
                #monitor the intraday price action
                exit_time = None
                exit_price = None

                #start: for setting the check interval to 5-minutes ###############################################################
                #NOTE: this 5 minute check is only applied to exit the trade (if stop-loss hit) and not for entry
                entry_data = entry_data.reset_index() # resetting the column to make timestamp a regular column
                entry_data_resampled_for_5_min = entry_data.resample('5T', on='timestamp').agg({'close': 'last'}).dropna()
                #end: for the 5-minute check code ###############################################################

                #for timestamp, row in entry_data.iterrows(): 
                #NOTE: To switch to 1-min check for exit, just replace entry_data_resampled_for_5_min with entry_data
                for timestamp, row in entry_data_resampled_for_5_min.iterrows():
                    current_price = row['close']
                    
                    if isinstance(current_price, pd.Series):
                        current_price = current_price.iloc[0] #handle ambiguity

                    #calling the trailing stop loss to get the udpated stop_loss (if applicable) and new highest price
                    # if (position == 'long' and current_price > highest_price) or (position == 'short' and current_price < highest_price) :
                    #     stop_loss, highest_price = update_trailing_stop_loss(current_price=current_price, highest_price=highest_price, atr=atr_value, trail_percent=trail_percent, position_type=position)

                    '''this applies only for 5x timestamp; for 1-min interval, comment the next 2 lines'''
                    time_part = timestamp.strftime('%H:%M:%S')
                    if time_part != '09:30:00' or time_part != '09:35:00': #skip the first 2 timestamps as our start time is 09:36
                        if position == 'long' and current_price < stop_loss:
                            exit_time = timestamp
                            exit_price = current_price
                            break #stop-loss hit, exit trade
                        elif position == 'short' and current_price > stop_loss:
                            exit_time = timestamp
                            exit_price = current_price
                            break #stop-loss hit, exit trade

                #if stop-loss wasn't hit, close at EOD
                if exit_time is None:
                    exit_time = end_time
                    exit_price = data.loc[end_time]['close']

                #log the closing of the trade
                log_trade('close', ticker, exit_price, exit_time, position)

                #store the postion
                positions.append({
                    'ticker': ticker,
                    'position': positions,
                    'entry_time': entry_time,
                    'entry_price': entry_price,
                    'stop_loss': stop_loss,
                    'closing_time': exit_time,
                    'closing_price': exit_price
                })

    return positions

#get each trading days from the top_daily_stocks.csv file
def get_unique_dates(file_path):
    df = pd.read_csv(file_path, parse_dates=['date'])
    unique_dates = df['date'].dt.date.unique()
    return sorted(unique_dates) #sort before returning

if __name__ == "__main__":

    #delete the log folder (all content of it) before running the script (to store new log files)
    folder_path = 'logs/'
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)

    unique_date = get_unique_dates(TOP_STOCKS_FILE)
    
    for trading_date in unique_date:
        # Process the trading for each day and get the list of positions
        print(f"Processing trading day: {trading_date}")

        trading_date_str = trading_date.strftime('%Y-%m-%d')
        positions = process_trading_day(trading_date_str)

# for test on a certain date
# if __name__ == "__main__":
#     positions = process_trading_day('2023-07-03')
