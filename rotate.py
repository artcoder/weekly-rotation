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
import sys

import pandas as pd
import pandas_ta as ta
import yfinance as yf

pickle_file_needs_to_be_updated = False

finish = datetime.date.today()
# finish = datetime.datetime(2021, 7, 6)
start = finish - timedelta(days=289)
print("Requested start:", start, " finish: ", finish)

# start = finish - timedelta(days=289)

# 253 trading days in a year
# 365.25 days in a year
# 200 trading days to look at
# How many calendar days?
# 200 * 365.25 / 253 = 289 calendar days

# You need to copy the S&P 500 companies into a CSV file
# https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
symbols_filename = 'C:\Data\code\sp500symbols.csv'

pickle_filename = 'C:\Data\code\stock_df.pkl'

extra_days = 5  # extra days to try to download in case the start date is not a trading day

# create empty dataframe
stock_df = pd.DataFrame()


# Using yfinance, download stock data on the stock symbols in symbols_filename
#  over the period start to end to the dataframe stock_df
def load_stock_data():

    global stock_df

    stock_list = []

    csvfile = open(symbols_filename, newline='')
    reader = csv.reader(csvfile)

    for row in reader:
        stock_list.append(row[0])

    for stock_symbol in stock_list:

        # print the symbol which is being downloaded
        print(str(stock_list.index(stock_symbol)) + str(':') + stock_symbol)

        try:
            # download the stock prices
            stock_data = []
            stock_data = yf.download(stock_symbol,
                                     start=(start - timedelta(days=extra_days)),
                                     end=(finish + timedelta(days=1)),
                                     progress=False)

            # append the individual stock prices 
            if len(stock_data) == 0:
                None
            else:
                stock_data['Name'] = stock_symbol
                stock_df = stock_df.append(stock_data, sort=False)
        except Exception:
            print("Could not download " + stock_symbol)
###


if pickle_file_needs_to_be_updated:
    load_stock_data()
    stock_df.to_pickle(pickle_filename)
else:
    pickle_file = open(pickle_filename, 'rb')
    stock_df = pickle.load(pickle_file)


stock_df = stock_df.reset_index()
# Just need "Adj Close", and "Volume", so drop the other columns
stock_df = stock_df.drop(columns=['Open', 'High', 'Low', 'Close'])
stock_df = stock_df.set_index(['Name', 'Date'])
stock_df = stock_df.sort_index()
#print(stock_df)


temp_df = stock_df.reset_index()
stocks = temp_df['Name'].unique()
# print(stocks)


# Correct start and finish dates so they are trading days
trading_days = stock_df.loc[stocks[0]].index  # "Date" is part of the MultiIndex

start_day_range = pd.date_range(start - timedelta(days=extra_days),
                                start).tolist()
start_day_range.reverse()
found_start_day = False

for d in start_day_range:
    if d in trading_days:
        start = d
        found_start_day = True
        break

if found_start_day == False:
    print('Could not find a trading day for the start day.')
    sys.exit(1)


finish_day_range = pd.date_range(finish - timedelta(days=extra_days),
                                 finish).tolist()
finish_day_range.reverse()
found_finish_day = False

for d in finish_day_range:
    if d in trading_days:
        finish = d
        found_finish_day = True
        break

if found_finish_day == False:
    print('Could not find a trading day for the finish day.')
    sys.exit(1)

print("Corrected start:", start, " finish: ", finish)


ROC = {}  # rate of change
RSI = {}  # relative strength index (3 days)
average_volume = {}


for stock in stocks:
    # print(stock)

    # Rate of change
    try:
        last_price = stock_df.loc[stock, finish].get("Adj Close")
    except KeyError:
        print("Failed to get finish price for " + stock)
        continue

    try:
        first_price = stock_df.loc[stock, start].get("Adj Close")
    except KeyError:
        print("Failed to get start price for " + stock)
        continue

    ROC[stock] = round( ((last_price - first_price) / first_price) * 100, 2)

    # Relative Strength Index (3 days)
    temp = ta.rsi(stock_df.loc[stock, :].get("Adj Close"), length=3)
    RSI[stock] = temp[finish]

    # Average Volume last 20 days
    temp = stock_df.loc[stock, :].get("Volume").rolling(window=20).mean()
    average_volume[stock] = temp[finish]

# print(ROC)
print("Highest Rate of Change% from start to finish:")
output = sorted(ROC.items(), key=operator.itemgetter(1), reverse=True)
# print(output)

count = 0
for i in output:
    print(i[0], i[1], '%,', end=" ")
    print('Vol:', average_volume[i[0]], end=" ")
    if average_volume[i[0]] > 1000000:
        print('OK', end=" ")
    else:
        print('!Low', end=" ")

    print(', RSI:', round(RSI[i[0]], 1), end=" ")
    if RSI[i[0]] < 50:
        print("OK")
    else:
        print("overbought")
    count = count + 1
    if count >= 10:
        break
