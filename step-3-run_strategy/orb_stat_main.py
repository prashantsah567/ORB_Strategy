'''start the trading strategy here and log all the result into a log file'''

'''
This is for backtesting. Trading session is from 9:30 to 16:00, so make sure you starting and ending between these time. Also ignore any comments:

1. Total Starting Capital -> $25,000 (only on first trading day, after that whatever it becomes either increases or decreases that would be considered as the new capital) 
2. The toatal capital will be allocated equally among all the picked stocks for that day
3. Commision rule -> $0.005/share (applied both when buying and selling)
4. When taking short position-> Commission + borrow fee (0.5% for now)
5. Now, first filter the stocks for the current trading day from the top_daily_stocks.csv file (you can store that into a list of tickers just for convinence and it will be updated each day), and after you get the tickers for the current day, you can get the data in the historical_data folder where each ticker data is stored in this format: ticker_1_min_data.csv
6. Check from 9:30 to 9:35 (check if close price is higher than open price on each candle, so start from 9:31, 9:32, 9:33, 9:34 and 9:35 - at least 4 of them should be in 1 direction either going high or low to take decision)
7. If the stock price from 9:30 to 9:35 remain higher in at least 4 of the candle (i.e. closing price higher than opening), we buy it (take long position) and if otherwise (means at least for 4 candles closing price is lower than opening) we take short position
8. We also put a stop loss along with our order with 10% of ATR (so it will be both for short or long position)
9. If the position is not stopped during the day (by 15:55 PM) close it at 15:56 PM
10. Closing the position -> Either on stoploss or profit at eod 
11. Also when you open a position or close a position, i want you to write the record in a file (don't create a new file for each trade, just one), mention: open/close a position at 'Stock Ticker' at price 'mention price here' at 'Time stamp'

other calculations:
1. Alpha
2. Beta
3. Sharpe ratio
4. Maximum Draw Down (MDD)
5. Volatility
6. Total Return in % and $
7. Final Capital
'''

import pandas as pd # type: ignore
from datetime import datetime
import os
import time
import pytz # type: ignore
import shutil

#constants
TOP_STOCKS_FILE = './step-2-get_candidate_stocks/top_qualified_daily_stocks_20_max.csv'
#HISTORICAL_DATA_FOLDER = 'historical_data'
PROCESSED_DATA_FOLDER = './processed_data_new'
LOG_FILE = 'trade_log_initial.csv'
STOP_LOSS_PERCENTAGE = 0.05 #5% of atr
atr_value = 0.15 #fixed atr_value for now (so stop loss'll be atr_value * STOP_LOSS_PERCENTAGE = 0.15 * 0.05 = 0.0075)
#trail_percent = 0.02 # 2%
ENTRY_PERCENTAGE_CHANGE = 0.015

#helper function to get tickers for a given date
def get_tickers_for_date(date):
    df = pd.read_csv(TOP_STOCKS_FILE)
    daily_stocks = df[df['date'] == date]
    tickers = daily_stocks['ticker'].tolist()

    return tickers

# load historical data for selected tickers (for .csv)
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

#new loading function based on processed parquet files
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

#check price movement between 9:31 AM and 9:35 AM
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

    if positive_movement >= 5:
        lowest_ob_price = data_filtered[['open', 'close']].min().min()
        return 'long', lowest_ob_price
    elif positive_movement <= 1:
        highest_ob_price = data_filtered[['open', 'close']].max().max()
        return 'short', highest_ob_price
    else:
        return 'no_trade',0

#calculate stop loss
def calculate_stop_loss(entry_price, atr, position_type):
    if position_type == 'long':
        return entry_price - (entry_price * STOP_LOSS_PERCENTAGE * atr)
    elif position_type == 'short':
        return entry_price + (entry_price * STOP_LOSS_PERCENTAGE * atr)

#trailing stop loss 
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
    #get tickers and ATR values for the selected day
    tickers = get_tickers_for_date(date)
    print(f'Tickers for {date}: {tickers}')

    #time_start = time.time()
    #load historical 1-min data for the ticker
    historical_data = load_historical_data(tickers)
    #print(f"total time taken to load historical data is: {time.time() - time_start}")

    #time_start = time.time()
    positions = [] #to store open positions for the day
    #check price movement for each ticker
    for ticker, data in historical_data.items():
        print(f"Analyzing {ticker}...")
        position, ob_price = check_price_movement(data, date)

        if position != 'no_trade':
            #start_time = pd.Timestamp(f"{date} 09:36:00-05:00")
            #end_time = pd.Timestamp(f"{date} 15:56:00-05:00")

            # Define the US/Eastern timezone using pytz
            eastern = pytz.timezone('US/Eastern')
            # Construct the start and end times dynamically based on the date
            start_time = pd.Timestamp(f"{date} 09:35:00").tz_localize(eastern, ambiguous='NaT')

            #checking for half day
            half_days = ['2022-07-03', '2023-07-03', '2023-11-24', '2024-07-03']

            if date in half_days:
                end_time = pd.Timestamp(f"{date} 12:55:00").tz_localize(eastern, ambiguous='NaT')
            else:
                end_time = pd.Timestamp(f"{date} 15:55:00").tz_localize(eastern, ambiguous='NaT')

            #filtering the trading time data for a certain day (1-minute interval - intraday)
            entry_data = data[(data.index >= start_time) & (data.index <= end_time)]

            if entry_data.empty:
                continue
            
            #regular entry price and time 09:35 (can change to 09:36 in line 180)
            # entry_time = entry_data.index[0]
            # entry_price = data.loc[entry_time,'close']

            entry_time_init = entry_data.index[0]
            entry_price_init = data.loc[entry_time_init,'close']

            entry_time = None
            entry_price = None

            #additional changes for setting entry_price as 2% below the 09:35 price
            for timestamp, row in entry_data.iterrows():
                current_price = row['close']

                if position == 'long' and current_price <= (entry_price_init - (entry_price_init * ENTRY_PERCENTAGE_CHANGE)):
                    entry_time = timestamp
                    entry_price = current_price
                    break #got the entry, exit out of loop
                elif position == 'short' and current_price >= (entry_price_init + (entry_price_init * ENTRY_PERCENTAGE_CHANGE)):
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
            # if isinstance (entry_price, pd.Series):
            #     entry_price = entry_price.iloc[0] #handle ambiguity
            
            # atr_value = data.loc[entry_time, 'ATR_14']
            # if atr_value > 0.3: #max_atr value is 0.3 which would limit max stop loss to 3%
            #     atr_value = 0.3

            #only go for any calculation or exit position if we entered a position
            if entry_price is not None:
                
                #so for the exit time, the timestamp need to start from the entry time (need to change the entry_data)
                entry_data = data[(data.index > entry_time) & (data.index <= end_time)] #start_time is entry_time now

                #calculate stop loss for long or short
                #stop_loss = calculate_stop_loss(entry_price, atr_value, position)
                stop_loss = calculate_stop_loss(entry_price, atr_value, position) #new stop loss

                #highest_price = entry_price #+ (entry_price * 0.015) #for long, this is the highest price; for short, it's the lowest

                #log the opening of the trade
                log_trade('open', ticker, entry_price, entry_time, position)
                
                #monitor the intraday price action
                exit_time = None
                exit_price = None

                #start: for setting the check interval to 5-minutes ###############################################################
                # entry_data = entry_data.reset_index() # resetting the column to make timestamp a regular column
                # '''there is a problem with this, basically it starting from 9:30 instead of next interval which should be 9:40 or so'''
                # entry_data_resampled_for_5_min = entry_data.resample('5T', on='timestamp').agg({'close': 'last'}).dropna()
                #end: for the 5-minute check code ###############################################################

                #for timestamp, row in entry_data.iterrows(): (if want to switch to 1-min, just replace entry_data_resampled_for_5_min with entry_data)
                for timestamp, row in entry_data.iterrows():
                    current_price = row['close']
                    
                    if isinstance(current_price, pd.Series):
                        current_price = current_price.iloc[0] #handle ambiguity

                    #calling the trailing stop loss to get the udpated stop_loss (if applicable) and new highest price
                    # if (position == 'long' and current_price > highest_price) or (position == 'short' and current_price < highest_price) :
                    #     stop_loss, highest_price = update_trailing_stop_loss(current_price=current_price, highest_price=highest_price, atr=atr_value, trail_percent=trail_percent, position_type=position)

                    '''this applies only for 5x timestamp'''
                    #get the time part only from timestamp
                    # time_part = timestamp.strftime('%H:%M:%S')
                    # if time_part != '09:30:00':

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

    #print(f"Time taken to run rest of the code and function after data is loaded: {time.time() - time_start}")

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

# Example usage - for quick test on a certain date

# if __name__ == "__main__":
#     positions = process_trading_day('2023-07-03')
