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
import operator
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
download = False

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

extra_days = 5  # extra days to try to download in case the start date is not a trading day


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
        print('No rows found in database table.')
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

    if download:
        data = yf.download(stock_list,
                           # start = download_start_date,
                           # end = download_finish_date,
                           start=(download_start_date - timedelta(days=extra_days)),
                           end=(download_finish_date + timedelta(days=1)),
                           group_by='ticker')

        data.to_pickle(pickle_filename)
    else:
        pickle_file = open(pickle_filename, 'rb')
        data = pickle.load(pickle_file)

    # https://stackoverflow.com/questions/63107594/how-to-deal-with-multi-level-column-names-downloaded-with-yfinance/63107801#63107801
    t_df = data.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index(level=1)
    t_df = t_df.reset_index()

    # insert dataframe data into database, but it fails if the date and ticker already exists
    # print(t_df)
    # t_df.to_sql('stock_data', con, if_exists='append', index=False)

    for i in range(len(t_df)):
        sql = 'insert into stock_data (date, ticker, close, high, low, open, volume) ' \
              'values (?,?,?,?,?,?,?)'
        try:
            cur.execute(sql, (t_df.iloc[i].get('Date').to_pydatetime(),
                              t_df.iloc[i].get('Ticker'),
                              t_df.iloc[i].get('Close'),
                              t_df.iloc[i].get('High'),
                              t_df.iloc[i].get('Low'),
                              t_df.iloc[i].get('Open'),
                              t_df.iloc[i].get('Volume') ))
        except sqlite3.IntegrityError:
            print("Failed inserting:", str(t_df.iloc[i][0]), t_df.iloc[i][1], end="\r")

    con.commit()
#


# detect_types is for timestamp support
con = sqlite3.connect(database_filename,
                      detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
cur = con.cursor()

# print("in main:", start_date, type(start_date))
download_start_date = find_download_start_date(start_date)

download_finish_date = finish_date

if download_start_date < download_finish_date:
    download_stock_data(download_start_date, download_finish_date)
else:
    print("Not downloading")

# Load requested date range from the database
sql = '''
Select * From stock_data
Where Date >= ? and Date <= ?
'''
cur.execute(sql,
            [start_date, finish_date])

stock_df = pd.DataFrame(cur.fetchall(),
                        columns=['date', 'ticker', 'open', 'high', 'low', 'close', 'volume'])
stock_df = stock_df.set_index(['ticker', 'date'])

# Find actual start date
query = '''
select date from stock_data
order by date
limit 1
'''
cur.execute(query)
t = cur.fetchone()
print("Database start date:", t[0])

# Find actual finish date
query = '''
Select date From stock_data
Order By date Desc
limit 1
'''
cur.execute(query)
t = cur.fetchone()
print("Database finish date:", t[0])

# Make a list of the stocks in the database
query = '''
SELECT DISTINCT ticker
FROM stock_data
'''
cur.execute(query)
t = cur.fetchall()

stocks = []
for stock in t:
    stocks.append(stock[0])

con.close()


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

# Calculate indicators
ROC = {}  # rate of change
RSI = {}  # relative strength index (3 days)
average_volume = {}

for stock in stocks:
    # print(stock)

    # Rate of change
    try:
        last_price = stock_df.loc[stock, finish_date].get("close")
    except KeyError:
        print("Failed to get finish price for " + stock)
        continue

    try:
        first_price = stock_df.loc[stock, start_date].get("close")
    except KeyError:
        print("Failed to get start price for " + stock)
        continue

    ROC[stock] = round(((last_price - first_price) / first_price) * 100, 2)

    # Relative Strength Index (3 days)
    temp = ta.rsi(stock_df.loc[stock, :].get("close"), length=3)
    RSI[stock] = temp[finish_date]

    # Average Volume last 20 days
    temp = stock_df.loc[stock, :].get("volume").rolling(window=20).mean()
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

    if RSI[i[0]] < 50:
        RSI_summary = 'OK'
    else:
        RSI_summary = 'Overbought'
        ranking = '!'

    if average_volume[i[0]] > 1000000:
        volume_summary = 'OK'
        count = count + 1  # only count higher volume stocks
    else:
        volume_summary = 'Low'
        ranking = 'X'

    print(ranking + ') ' + stock_symbol + ' ' + rate_of_change + '%', end='')
    print(' Volume ' + volume_summary + ': ' + str(average_volume[i[0]]) + ', ', end='')
    print('RSI ' + RSI_summary + ': ' + str(round(RSI[i[0]], 1)))

    if count >= 11:
        break
