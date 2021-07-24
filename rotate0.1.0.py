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
import pickle

database_filename = 'stock_data.sqlite3'
symbols_filename = r'C:\Data\code\sp500symbols.csv'
pickle_filename = r'.\stock_df_0.1.0.pkl'
download = True

# Set requested date range
finish_date = datetime.date.today()
# finish_date = datetime.datetime(2021, 7, 6)
start_date = finish_date - timedelta(days=289)
print("Requested start:", start_date, "finish:", finish_date)

# start = finish - timedelta(days=289)
# 253 trading days in a year
# 365.25 days in a year
# 200 trading days to look at
# How many calendar days?
# 200 * 365.25 / 253 = 289 calendar days

extra_days = 0  # 5  # extra days to try to download in case the start date or finish date is not a trading day


def find_download_start_date(requested_start_date):
    global con
    # print("In find_download_start_date:", requested_start_date, type(requested_start_date))
    cur = con.cursor()

    # If table does not exist, create it
    sql = '''
    CREATE TABLE  IF NOT EXISTS stock_data
    (date timestamp NOT NULL,
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
    Order By date Desc
    Limit 1
    '''

    cur.execute(sql)
    rows = cur.fetchall()

    # if no date
    if len(rows) < 1:
        print('No rows found in database.')
        download_start_date = requested_start_date
    else:
        print('Last date found in database:', rows[0][0])
        # Download the day after the one in the database
        download_start_date = rows[0][0].date() + timedelta(days=1)

    return download_start_date


def download_stock_data(download_start_date, download_finish_date):
    global con

    print("Download_stock_data: start date:", download_start_date, "finish date:", download_finish_date)
    stock_list = []
    csvfile = open(symbols_filename, newline='')
    reader = csv.reader(csvfile)

    for row in reader:
        stock_list.append(row[0])

    # debug
    # stock_list = ['A', 'B']

    if download:
        data = yf.download(stock_list,
                           # start = download_start_date,
                           # end = download_finish_date,
                           start=(download_start_date - timedelta(days=extra_days)),
                           end=(download_finish_date + timedelta(days=1)),
                           group_by='ticker',
                           auto_adjust=True)

        data.to_pickle(pickle_filename)
    else:
        pickle_file = open(pickle_filename, 'rb')
        data = pickle.load(pickle_file)

    # https://stackoverflow.com/questions/63107594/how-to-deal-with-multi-level-column-names-downloaded-with-yfinance/63107801#63107801
    t_df = data.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index(level=1)
    t_df = t_df.reset_index()

    # insert dataframe data into database
    # print(t_df)
    # t_df.to_sql('stock_data', con, if_exists='append', index=False)

    for i in range(len(t_df)):
        sql = 'insert into stock_data values (?,?,?,?,?,?,?)'
        try:
            cur.execute(sql, (t_df.iloc[i][0].to_pydatetime(), t_df.iloc[i][1], t_df.iloc[i][2], t_df.iloc[i][3],
                              t_df.iloc[i][4], t_df.iloc[i][5], t_df.iloc[i][6]))
        except sqlite3.IntegrityError:
            print("Failed inserting:", str(t_df.iloc[i][0]), t_df.iloc[i][1], end="\r")

    con.commit()
#


con = sqlite3.connect(database_filename,
                      detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
# for timestamp support
cur = con.cursor()

# print("in main:", start_date, type(start_date))
download_start_date = find_download_start_date(start_date)

download_finish_date = finish_date

if download_start_date < download_finish_date:
    download_stock_data(download_start_date, download_finish_date)
else:
    print("Not downloading")

# Load requested date range from db
sql = '''
Select * From stock_data
Where Date >= ? and Date <= ?
'''
cur.execute(sql,
            [start_date, finish_date])

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
print("database start date:", t[0])

# find actual finish date
query = '''
Select date From stock_data
Order By date Desc
limit 1
'''
cur.execute(query)
t = cur.fetchone()
print("database finish date:", t[0])

con.close()

# calculate indicators

# show results
