# Idea from /The 30-Minute Stock Trader/ by Laurens Bensdorp
# "Chapter 8 Weekly Rotation S&P 500 -- For the Busy or Lazy"
#
# The download code was based on:
# https://towardsdatascience.com/downloading-historical-stock-prices-in-python-93f85f059c1f
###
# David Guilbeau

import csv
import datetime
import operator
import pickle
from datetime import timedelta

import pandas as pd
import pandas_ta as ta
import yfinance as yf

finish = datetime.datetime(2021, 7, 2)
start = finish - timedelta(days=289)
# 253 trading days in a year
# 365.25 days in a year
# 200 trading days to look at
# how many calendar days?
# 200 * 365.25 / 253 = 289 calendar days

# You need to copy the S&P 500 companies into a CSV file
# https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
symbols_filename = 'C:\Data\code\sp500symbols.csv'

pickle_filename = 'C:\Data\code\stock_df.pkl'

# create empty dataframe
stock_df = pd.DataFrame()


def load_stock_data():

    global stock_df

    stocks = []

    csvfile = open(symbols_filename, newline='')
    reader = csv.reader(csvfile)
    
    for row in reader:
        stocks.append(row[0])

    for s in stocks:
    
        # print the symbol which is being downloaded
        print(str(stocks.index(s)) + str(':') + s)
    
        try:
            # download the stock prices
            stock = []
            stock = yf.download(s, start=start, end=finish, progress=False)
        
            # append the individual stock prices 
            if len(stock) == 0:
                None
            else:
                stock['Name'] = s
                stock_df = stock_df.append(stock, sort=False)
        except Exception:
            print("Could not download " + s)


pickle_file = open(pickle_filename, 'rb')
stock_df = pickle.load(pickle_file)

# if loading_pickle_file_fails:
#load_stock_data()
#stock_df.to_pickle(pickle_filename)

stock_df = stock_df.reset_index()
# Just need "Adj Close", and "Volume"
stock_df = stock_df.drop(columns=['Open', 'High', 'Low', 'Close'])
stock_df = stock_df.set_index(['Name', 'Date'])
stock_df = stock_df.sort_index()

#print("length=", len(stock_df.loc["AAPL"]))
#print(stock_df.iloc[0])
#print(stock_df.iloc[251])
#print(stock_df.loc["AAPL"].iloc[51])
#print(stock_df.loc["AAPL"].iloc[251])
#print(stock_df.loc["AAPL"])

temp_df = stock_df.reset_index()
stocks = temp_df['Name'].unique()
# print(stocks)

ROC = {}
RSI = {}
for stock in stocks:
    # print(stock)
    try:
        last_price = stock_df.loc[stock, finish - timedelta(days=1)].get("Adj Close")
        first_price = stock_df.loc[stock, start].get("Adj Close")
        ROC[stock] = round(((last_price - first_price) / first_price) * 100,2)

        temp = ta.rsi(stock_df.loc[stock, :].get("Adj Close"), length=3)
        RSI[stock] = temp[finish - timedelta(days=1)]

    except:
        print("Failed to get start or finish price for " + stock)
        continue

# print(ROC)
print("start:", start, " finish: ", finish)
print("Highest Rate of Change% from start to finish:")
output = sorted(ROC.items(), key=operator.itemgetter(1), reverse=True)
# print(output)

count = 0
for i in output:
    print(i[0], i[1], round(RSI[i[0]], 1), end=" ")
    if RSI[i[0]] < 50:
        print("OK")
    else:
        print("overbought")
    count = count + 1
    if count >= 10:
        break

#print(stock_df.loc["AAPL", :].get("Adj Close"))
#RSI = {}
#RSI = ta.rsi(stock_df.loc["AAPL", :].get("Adj Close"), length=5)
#print(type(RSI))
#print(RSI)
#print(RSI[finish - timedelta(days=1)])

#stock="AAPL"
#print(stock_df.loc[stock, finish - timedelta(days=1)].get("Adj Close"))
#print(stock_df.loc[stock, start].get("Adj Close"))