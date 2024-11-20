import pandas as pd
import numpy as np
import os
import pytz

# File paths
log_file = "logs/trade_log.csv"  # Path to trade log file
output_file = "logs/final_metrics.csv"  # Path to save final results
trade_details_file = "logs/trade_details.csv" #file for detailed trade data

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
        commission = 0.005 * shares
        borrow_fee = 0.005 * capital_per_stock if position_type == "short" else 0

        # Calculate profit/loss
        if position_type == "long":
            trade_profit = ((close_price - open_price) * shares - 2 * commission - borrow_fee)
        else: 
            trade_profit = ((open_price - close_price) * shares - 2 * commission - borrow_fee)

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

# Save metrics to file
metrics_df = pd.DataFrame([metrics])
metrics_df.to_csv(output_file, index=False)

print(f"Metrics saved to {output_file}")
print(f"Trade details saved to {trade_details_file}")