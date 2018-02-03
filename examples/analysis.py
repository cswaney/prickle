import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from hfttools import nodups

books = pd.read_csv('/Volumes/datasets/ITCH/csv/111413/books/books_A.txt')
messages = pd.read_csv('/Volumes/datasets/ITCH/csv/111413/messages/messages_A.txt')
hidden = pd.read_csv('/Volumes/datasets/ITCH/csv/111413/trades/trades_A.txt')
books, messages = nodups(books, messages)
books['time'] = books['sec'] + books['nano'] / 10 ** 9
messages['time'] = messages['sec'] + messages['nano'] / 10 ** 9
hidden['time'] = hidden['sec'] + hidden['nano'] / 10 ** 9
books = books[(books['time'] > 34200) & (books['time'] < 57000)]
messages = messages[(messages['time'] > 34200) & (messages['time'] < 57000)]
hidden = hidden[(hidden['time'] > 34200) & (hidden['time'] < 57000)]

# Basics
plt.plot(books['time'], books['bidprc0'])
plt.plot(books['time'], books['askprc0'])

books['bidvol0'].describe()
books['askvol0'].describe()

spread = books['askprc0'] - books['bidprc0']
spread.describe()
plt.plot(books['time'], spread)

midprice = (books['bidprc0'] + books['askprc0']) / 2
midprice.describe()

messages.loc[:,('shares', 'price')].describe()
pd.value_counts(messages['type'])
pd.value_counts(messages['mpid'])


# Trades
trades = find_trades(messages)
pd.value_counts(trades['side'])
pd.value_counts(trades['hit'])
trades.loc[:,('shares', 'vwap', 'hit')].describe()
buys = trades[trades['side'] == 'B']
sells = trades[trades['side'] == 'S']
plt.scatter(buys['time'], buys['vwap'], alpha=0.50, s=8, color='C4')
plt.scatter(sells['time'], sells['vwap'], alpha=0.50, s=8, color='C2')
plt.legend(['buys', 'sells'])
plt.bar(buys['time'], -buys['shares'], alpha=0.5, color='C0', width=10)
plt.bar(sells['time'], sells['shares'], alpha=0.5, color='C1', width=10)
plt.legend(['buys', 'sells'])

# Hidden
buys = hidden[hidden['side'] == 'S']
sells = hidden[hidden['side'] == 'B']
plt.scatter(buys['time'], buys['price'], alpha=0.25, s=8)
plt.scatter(sells['time'], sells['price'], alpha=0.25, s=8)
hidden.loc[:,('shares', 'price')].describe()
pd.value_counts(hidden['side'])
hidden_trades = find_trades(hidden)
hidden_trades.loc[:,('shares', 'price', 'hit')].describe()
