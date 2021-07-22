# Python code to automate a stock trading strategy
# from /The 30-Minute Stock Trader/ by Laurens Bensdorp
# "Chapter 8 Weekly Rotation S&P 500 -- For the Busy or Lazy"
#
# The download code was based on:
# https://towardsdatascience.com/downloading-historical-stock-prices-in-python-93f85f059c1f
###
# David Guilbeau
# Version 0.1.0

import csv
import datetime
from datetime import timedelta
import sys
import pandas as pd
import pandas_ta as ta
import yfinance as yf
import sqlite3
import traceback

database_filename = 'stock_data.sqlite3'
symbols_filename = r'C:\Data\code\sp500symbols.csv'

# Set requested date range
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

extra_days = 5  # extra days to try to download in case the start date or finish date is not a trading day


def find_download_start_date(requested_start_date):
    global con
    cur = con.cursor()

    # If table does not exist, create it
    sql = '''
	CREATE TABLE  IF NOT EXISTS stock_data
	(date text NOT NULL,
	ticker text NOT NULL,
	open real,
	high real,
	low real,
	close real,
	volume real,
	primary key(date, ticker)
	)
	'''
    cur.execute(sql)

    # Find the last date in the db:
    sql = '''
	Select date From stock_data
	Order By date
	Limit 1
	'''
    cur.execute(sql)
    rows = cur.fetchall()

    # if no date
    if len(rows) < 1:
        download_start_date = requested_start_date
    else:
        download_start_date = rows[0][0]

    return download_start_date


def download_stock_data(download_start_date, download_finish_date):
    global con

    stock_list = []
    csvfile = open(symbols_filename, newline='')
    reader = csv.reader(csvfile)

    for row in reader:
        stock_list.append(row[0])

    data = yf.download(stock_list,
                       # start = download_start_date,
                       # end = download_finish_date,
                       start=(download_start_date - timedelta(days=extra_days)),
                       end=(download_finish_date + timedelta(days=1)),
                       group_by='ticker',
                       auto_adjust=True)

    t_df = data.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index(level=1)
    t_df = t.reset_index()

    # insert dataframe data into database
    t_df.to_sql('stock_data', con, if_exists='append', index=False)


con = sqlite3.connect(database_filename,
                      detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
cur = con.cursor()

download_start_date = find_download_start_date(start_date)

download_finish_date = finish_date

download_stock_data(download_start_date, download_finish_date)

# Load requested date range from db
sql = '''
Select * From stock_data
Where Date >= ? and Date <= ?
'''
cur.execute(sql, start_date, finish_date)

ticker_df = pd.DataFrame(cur.fetchall(),
                         columns=['date', 'ticker', 'open', 'high', 'low', 'close', 'volume'])

# find actual start date
query = '''
select date from stock_data
order by date
limit 1
'''
cur.execute(query)
t = cur.fetchone()
print("start date:", t[0])

# find actual finish date
query = '''
Select date From stock_data
Order By date Desc
limit 1
'''
cur.execute(query)
t = cur.fetchone()
print("finish date:", t[0])

# calculate indicators

# show results
