# ORB_STRATEGY 

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