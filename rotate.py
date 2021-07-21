# Python code to automate a stock trading strategy
# from /The 30-Minute Stock Trader/ by Laurens Bensdorp
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
import traceback

pickle_file_needs_to_be_updated = True

finish_date = datetime.date.today()
# finish_date = datetime.datetime(2021, 7, 6)
start_date = finish_date - timedelta(days=289)
print("Requested start:", start_date, " finish: ", finish_date)

# start = finish - timedelta(days=289)

# 253 trading days in a year
# 365.25 days in a year
# 200 trading days to look at
# How many calendar days?
# 200 * 365.25 / 253 = 289 calendar days

# You need to copy the S&P 500 companies into a "CSV" file. Just have every stock symbol on it's own line
# https://en.wikipedia.org/wiki/List_of_S%26P_500_companies
symbols_filename = r'.\sp500symbols.csv'

pickle_filename = r'.\stock_df.pkl'

extra_days = 5  # extra days to try to download in case the start date or finish date is not a trading day

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
            # end=(finish_date + timedelta(days=1)),
            stock_data = yf.download(stock_symbol,
                                     start=(start_date - timedelta(days=extra_days)),
                                     end=(finish_date + timedelta(days=1)),
                                     threads=False,
                                     progress=False)

            # append the individual stock prices
            if len(stock_data) == 0:
                print("No stock data returned for", stock_symbol)
            else:
                stock_data['Name'] = stock_symbol
                stock_df = stock_df.append(stock_data, sort=False)

        except Exception as e:
            traceback.print_exc()
            print(e)
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


temp_df = stock_df.reset_index()
stocks = temp_df['Name'].unique()
# print(stocks)


# Correct start and finish dates so they are trading days
trading_days = stock_df.loc[stocks[0]].index  # "Date" is part of the MultiIndex

start_day_range = pd.date_range(start_date - timedelta(days=extra_days),
                                start_date).tolist()
start_day_range.reverse()
found_start_day = False

for d in start_day_range:
    if d in trading_days:
        start_date = d
        found_start_day = True
        break

if found_start_day == False:
    print('Could not find a trading day for the start day.')
    sys.exit(1)


finish_day_range = pd.date_range(finish_date - timedelta(days=extra_days),
                                 finish_date).tolist()
finish_day_range.reverse()
found_finish_day = False

for d in finish_day_range:
    if d in trading_days:
        finish_date = d
        found_finish_day = True
        break

if found_finish_day == False:
    print('Could not find a trading day for the finish day.')
    sys.exit(1)

print("Corrected start:", start_date, " finish: ", finish_date)


ROC = {}  # rate of change
RSI = {}  # relative strength index (3 days)
average_volume = {}


for stock in stocks:
    # print(stock)

    # Rate of change
    try:
        last_price = stock_df.loc[stock, finish_date].get("Adj Close")
    except KeyError:
        print("Failed to get finish price for " + stock)
        continue

    try:
        first_price = stock_df.loc[stock, start_date].get("Adj Close")
    except KeyError:
        print("Failed to get start price for " + stock)
        continue

    ROC[stock] = round( ((last_price - first_price) / first_price) * 100, 2)

    # Relative Strength Index (3 days)
    temp = ta.rsi(stock_df.loc[stock, :].get("Adj Close"), length=3)
    RSI[stock] = temp[finish_date]

    # Average Volume last 20 days
    temp = stock_df.loc[stock, :].get("Volume").rolling(window=20).mean()
    average_volume[stock] = temp[finish_date]

# print(ROC)
print("Highest Rate of Change% from start to finish:")
output = sorted(ROC.items(), key=operator.itemgetter(1), reverse=True)
# print(output)

ranking = ''
count = 1
for i in output:
    ranking = str(count)
    stock_symbol = i[0]
    rate_of_change = str(i[1])
    volume_summary = ''

    if average_volume[i[0]] > 1000000:
        volume_summary = 'OK'
        count = count + 1  # only count higher volume stocks
    else:
        volume_summary = 'Low'
        ranking = '!'

    if RSI[i[0]] < 50:
        RSI_summary = 'OK'
    else:
        RSI_summary = 'Overbought'
        ranking = '!'

    print(ranking + ') ' + stock_symbol + ' ' + rate_of_change + '%', end='')
    print(' Volume ' + volume_summary + ': ' + str(average_volume[i[0]]) + ', ', end='')
    print('RSI ' + RSI_summary + ': ' + str(round(RSI[i[0]], 1)) )

    if count >= 11:
        break
