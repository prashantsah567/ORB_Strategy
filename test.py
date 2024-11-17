import pandas as pd # type: ignore

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

#to check how many times a value is repeated
df = pd.read_csv('top_daily_stocks.csv')
tickers_count = df['ticker'].value_counts()

print(tickers_count)
print(f'\n{tickers_count.sum()}')

"""
META    351
NFLX    253
TSLA    104
MSFT    101
CRWD     76
UNH      73
AMD      66
ADBE     45
AVGO     29
NVDA     14
QCOM     12
"""