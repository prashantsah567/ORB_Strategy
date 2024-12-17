import pandas as pd  # type: ignore
import numpy as np # type: ignore
import os
import pytz # type: ignore

# File paths
log_file = "logs/trade_log_initial.csv"  # Path to trade log file which will be used for trade calculation
metrics_output_file = "logs/final_metrics.csv"  # Path to save final results
trade_details_file = "logs/trade_details.csv" #file for detailed trade logs

#delete the previously generated log file (trade_details); final_metrics file replaces its value so no need to delete
if os.path.exists(trade_details_file):
    os.remove(trade_details_file)

# Initialize variables
starting_capital = 100000
capital = starting_capital
risk_free_rate = 0.03  # Example risk-free rate (adjust if needed)
daily_returns = []  # To track daily returns
current_date = None
metrics = {
    "Alpha": 0,
    "Beta": 0,
    "Sharpe Ratio": 0,
    "Max Drawdown": 0,
    "Volatility": 0,
    "Total Return (%)": 0,
    "Final Capital": 0
}

# Read the trade log file
log_df = pd.read_csv(log_file)
log_df['timestamp'] = pd.to_datetime(log_df['timestamp'], errors='coerce', utc=True)

# Convert UTC timestamps to US/Eastern
eastern = pytz.timezone('US/Eastern')
log_df['timestamp'] = log_df['timestamp'].dt.tz_convert(eastern)
log_df['date'] = log_df['timestamp'].dt.date

# Initialize the trade details file
if not os.path.exists(trade_details_file):
    pd.DataFrame(columns=[
        "ticker", "position_type", "entry_time", "exit_time", "entry_price", "exit_price", "capital_allocated", "shares_traded", 
        "profit/loss", "% of profit/loss", "updated_capital"
    ]).to_csv(trade_details_file, index=False)

# Process trades day by day
for date, daily_data in log_df.groupby('date'):
    # Get unique tickers for the day
    unique_tickers = daily_data['ticker'].unique()

    # Capital allocation for the day
    capital_per_stock = capital / len(unique_tickers) if len(unique_tickers) > 0 else 0

    # Track day-level returns
    daily_return = 0
    for ticker in unique_tickers:
        ticker_trades = daily_data[daily_data['ticker'] == ticker]

        # Get open and close trades
        open_trade = ticker_trades[ticker_trades['status'] == 'open']
        close_trade = ticker_trades[ticker_trades['status'] == 'close']

        if open_trade.empty or close_trade.empty:
            continue

        # Calculate shares, commission, and borrow fees
        open_price = open_trade.iloc[0]['price']
        close_price = close_trade.iloc[0]['price']
        entry_time = open_trade.iloc[0]['timestamp']
        exit_time = close_trade.iloc[0]['timestamp']
        position_type = open_trade.iloc[0]['position_type']
        shares = capital_per_stock / open_price
        commission = 0.0035 * shares
        #since we are not holding the position overnight, there is no borrow fee
        #borrow_fee = 0.005 * capital_per_stock if position_type == "short" else 0

        # Calculate profit/loss
        if position_type == "long":
            trade_profit = ((close_price - open_price) * shares - 2 * commission) # - borrow_fee)
        else: 
            trade_profit = ((open_price - close_price) * shares - 2 * commission) # - borrow_fee)

        # Update capital and returns
        capital += trade_profit
        daily_return += trade_profit / capital_per_stock

        #% of profit/loss
        profit_loss_percent = (trade_profit/capital_per_stock) * 100 if capital_per_stock > 0 else 0

        # Append trade details
        trade_details = {
            "ticker": ticker,
            "position_type": position_type,
            "entry_time": entry_time,
            "exit_time": exit_time,
            "entry_price": open_price,
            "exit_price": close_price,
            "capital_allocated": capital_per_stock,
            "shares_traded": shares,
            "profit/loss": trade_profit,
            "% of profit/loss": profit_loss_percent,
            "updated_capital": capital
        }

        pd.DataFrame([trade_details]).to_csv(trade_details_file, mode='a', header=False, index=False)

    daily_returns.append(daily_return)

# Calculate metrics
metrics["Final Capital"] = capital
metrics["Total Return (%)"] = ((capital - starting_capital) / starting_capital) * 100
metrics["Volatility"] = np.std(daily_returns)
metrics["Sharpe Ratio"] = (np.mean(daily_returns) - risk_free_rate) / metrics["Volatility"]

# Save metrics to metrics_output_file
metrics_df = pd.DataFrame([metrics])
metrics_df.to_csv(metrics_output_file, index=False)

#print the results ######################################################################
print(f"Final Capital = {round(capital,2)}")
total_percent_return = round(((capital - starting_capital) / starting_capital) * 100,2)
print(f"Total % Return =  {total_percent_return}")
df = pd.read_csv('logs/trade_details.csv') #read the trade details file for return based on position type (long and short)
result = df.groupby('position_type')['% of profit/loss'].agg(['sum', 'count']).reset_index()
result.columns = ['position_type', '(%)_return', 'Num_of_Trades'] #rename the columns for readability 
print(result)

#extract values from result
long_return = round(result.loc[result['position_type'] == 'long', '(%)_return'].values[0],2)
short_return = round(result.loc[result['position_type'] == 'short', '(%)_return'].values[0],2)
long_trades = result.loc[result['position_type'] == 'long', 'Num_of_Trades'].values[0]
short_trades = result.loc[result['position_type'] == 'short', 'Num_of_Trades'].values[0]

#append the final result to a .csv file (for testing) #######################################################
import sys
sys.path.append('./step-3-run_strategy')
from orb_stat_main import STOP_LOSS_PERCENTAGE, atr_value, ENTRY_PERCENTAGE_CHANGE # type: ignore

test_result_data = {
    'stop_loss_percent': STOP_LOSS_PERCENTAGE,
    'atr_value' : atr_value,
    'entry_%_change (X 100)': ENTRY_PERCENTAGE_CHANGE,
    'long_%_return': long_return,
    'total_long_trades': long_trades,
    'short_%_return': short_return,
    'total_short_trades': short_trades,
    'total_%_return': total_percent_return,
    'final_capital': round(capital,2)
}

result_df = pd.DataFrame([test_result_data])
csv_file = 'step-4-result/test_results.csv'

if not os.path.isfile(csv_file):
    result_df.to_csv(csv_file, index=False)
else:
    result_df.to_csv(csv_file, mode='a', header=False, index=False)
