'''start the trading strategy here'''
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
import pytz

#constants
CAPITAL = 25000
TOP_STOCKS_FILE = 'top_daily_stocks.csv'
HISTORICAL_DATA_FOLDER = 'historical_data'
PROCESSED_DATA_FOLDER = 'processed_data'
STOP_LOSS_PERCENTAGE = 0.10 #10%
LOG_FILE = 'trade_log.csv'

#helper function to get tickers for a given date
def get_tickers_for_date(date):
    df = pd.read_csv(TOP_STOCKS_FILE)
    daily_stocks = df[df['date'] == date]
    tickers = daily_stocks['ticker'].tolist()
    atr_values = dict(zip(daily_stocks['ticker'], daily_stocks['ATR_14']))

    return tickers, atr_values

# load historical data for selected tickers
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
    start_time = pd.Timestamp(f"{date} 09:31:00").tz_localize(eastern, ambiguous='NaT')
    end_time = pd.Timestamp(f"{date} 09:35:00").tz_localize(eastern, ambiguous='NaT')
    
    try:
        data_filtered = data.loc[start_time:end_time]
    except KeyError as e:
        print(f"Missing data for the range {start_time} to {end_time}.") #for any missing timestamp
        return 'no_trade'

    #count how many of the 5 candles had close > open
    positive_movement = sum(data_filtered['close'] > data_filtered['open'])

    if positive_movement >= 4:
        return 'long'
    elif positive_movement <=1:
        return 'short'
    else:
        return 'no_trade'

#calculate stop loss
def calculate_stop_loss(entry_price, atr, position_type): #**need to re-calculate the atr as the atr we have is from 9:30 but our entry time 9:36
    if position_type == 'long':
        return entry_price - (STOP_LOSS_PERCENTAGE * atr)
    elif position_type == 'short':
        return entry_price + (STOP_LOSS_PERCENTAGE * atr)

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
    tickers, atr_values = get_tickers_for_date(date)
    print(f'Tickers for {date}: {tickers}')

    time_start = time.time()
    #load historical 1-min data for the ticker
    historical_data = load_historical_data(tickers)
    print(f"total time taken to load historical data is: {time.time() - time_start}")

    time_start = time.time()
    positions = [] #to store open positions for the day
    #check price movement for each ticker
    for ticker, data in historical_data.items():
        print(f"Analyzing {ticker}...")
        position = check_price_movement(data, date)

        if position != 'no_trade':
            #start_time = pd.Timestamp(f"{date} 09:36:00-05:00")
            #end_time = pd.Timestamp(f"{date} 15:56:00-05:00")

            # Define the US/Eastern timezone using pytz
            eastern = pytz.timezone('US/Eastern')
            # Construct the start and end times dynamically based on the date
            start_time = pd.Timestamp(f"{date} 09:36:00").tz_localize(eastern, ambiguous='NaT')
            end_time = pd.Timestamp(f"{date} 15:56:00").tz_localize(eastern, ambiguous='NaT')

            entry_data = data[(data.index >= start_time) & (data.index <= end_time)]

            if entry_data.empty:
                continue

            entry_time = entry_data.index[0]
            entry_price = data.loc[entry_time,'close']
            if isinstance (entry_price, pd.Series):
                entry_price = entry_price.iloc[0]

            #calculate stop loss for long or short
            stop_loss = calculate_stop_loss(entry_price, atr_values[ticker], position)

            #log the opening of the trade
            log_trade('open', ticker, entry_price, entry_time, position)
            
            #monitor the intraday price action
            exit_time = None
            exit_price = None

            for timestamp, row in entry_data.iterrows():
                current_price = row['close']
                
                if isinstance(current_price, pd.Series):
                    current_price = current_price.iloc[0] #handle ambiguity

                #additional print statement
                #print(f"Processing {ticker} at {timestamp}: current_price={current_price}, stop_loss={stop_loss}")

                if position == 'long' and current_price <= stop_loss:
                    exit_time = timestamp
                    exit_price = current_price
                    break #stop-loss hit, exit trade
                elif position == 'short' and current_price >= stop_loss:
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

    print(f"Time taken to run rest of the code and function after data is loaded: {time.time() - time_start}")

    return positions

#get each trading days from the top_daily_stocks.csv file
def get_unique_dates(file_path):
    df = pd.read_csv(file_path, parse_dates=['date'])
    unique_dates = df['date'].dt.date.unique()
    return sorted(unique_dates) #sort before returning

if __name__ == "__main__":

    unique_date = get_unique_dates(TOP_STOCKS_FILE)
    
    for trading_date in unique_date:
        # Process the trading for each day and get the list of positions
        print(f"Processing trading day: {trading_date}")

        trading_date_str = trading_date.strftime('%Y-%m-%d')
        positions = process_trading_day(trading_date_str)

# Example usage - for quick test on a certain date

# if __name__ == "__main__":
#     positions = process_trading_day('2022-12-16')

'''
1. ValueError: The truth value of a Series is ambiguous. Use a.empty, a.bool(), a.item(), a.any() or a.all().
at if position == 'long' and current_price <= stop_loss: at 2023-03-02 09:36:00-05:00    173.29
2. why loading historical_data is taking too long, can't it be done just by going to that folder
3. Calculate all the values, check if it can be done from the log file or not
4. Tweak various parameters to find max result
'''