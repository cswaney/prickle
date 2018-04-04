import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from hfttools import nodups

"""Aggregate Analysis"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# message counts
message_counts = pd.read_csv('/Volumes/datasets/ITCH/stats/message_counts.txt', index_col=0)
message_counts = message_counts.dropna()
sums = message_counts.loc[:,'A':'X'].sum()
plt.bar(np.arange(len(sums)), sums / sums.sum(), color='C0')
plt.xticks(np.arange(len(sums)), ['A', 'C', 'D', 'E', 'F', 'U', 'X'])
plt.show()
sums_counts = sums

# message shares
# message_shares = pd.read_csv('/Volumes/datasets/ITCH/stats/message_shares.txt', index_col=0)
# message_shares = message_shares.dropna()
# grouped = message_shares.groupby('type')
# sums = grouped.sum().drop('date', axis=1)
# ax = plt.subplot()
# # for label, i in zip(['A', 'C', 'D', 'E', 'F', 'U', 'X'], np.arange(7)):
# for label, i in zip(['A', 'D', 'E'], np.arange(7)):
#     bins = sums.loc[label, :]
#     ax.plot([int(x) for x in bins.index], bins / bins.sum(), color='C{}'.format(i))
# ax.grid(True, axis='y', linestyle='dashed')
# ax.tick_params(direction='in', which='both')
# # ax.legend(['A', 'C', 'D', 'E', 'F', 'U', 'X'], ncol=7, loc='upper center')
# ax.legend(['Add', 'Delete', 'Execute'], ncol=7, loc='upper center')
# plt.show()

# message_times
hours = pd.date_range(start='01-02-2013 9:30:00', end='01-02-2013 16:00:00', freq='H')
message_times = pd.read_csv('/Volumes/datasets/ITCH/stats/message_times.txt', index_col=0)
message_times = message_times.dropna()
message_nano = pd.read_csv('/Volumes/datasets/ITCH/stats/message_nano.txt', index_col=0)
message_nano = message_nano.dropna()

ax = plt.subplot(211)
grouped = message_times.groupby('type')
sums = grouped.sum().drop('date', axis=1)
colors = [0,3,2]
# for label, i in zip(['A', 'C', 'D', 'E', 'F', 'U', 'X'], np.arange(7)):
for label, i in zip(['A', 'D', 'E'], np.arange(3)):
    bins = sums.loc[label, :]
    ax.plot([int(x) for x in bins.index], bins / bins.sum(), color='C{}'.format(colors[i]))
ax.tick_params(direction='in', which='both')
# ax.legend(['A', 'C', 'D', 'E', 'F', 'U', 'X'], ncol=7, loc='upper center')
ax.legend(['Add', 'Delete', 'Execute'])
plt.xlim([34200, 57300])
plt.xticks(np.arange(34200, 57600, 3600), [h.strftime('%H:%M') for h in hours], rotation=45)

ax = plt.subplot(212)
grouped = message_nano.groupby('type')
sums = grouped.sum().drop('date', axis=1)
colors = [0,3,2]
# for label, i in zip(['A', 'C', 'D', 'E', 'F', 'U', 'X'], np.arange(7)):
for label, i in zip(['A', 'D', 'E'], np.arange(3)):
    bins = sums.loc[label, :]
    ax.plot([int(x)/10**9 for x in bins.index], bins / bins.sum(), color='C{}'.format(colors[i]))
ax.tick_params(direction='in', which='both')
# ax.legend(['A', 'C', 'D', 'E', 'F', 'U', 'X'], ncol=7, loc='upper center')
# ax.legend(['Add', 'Delete', 'Execute'])
plt.xlim([0, 0.98])
plt.show()

# trades
trades = pd.read_csv('/Volumes/datasets/ITCH/stats/trades.txt', index_col=0)
hidden_trades = pd.read_csv('/Volumes/datasets/ITCH/stats/hidden_trades.txt', index_col=0)
h,x = np.histogram(-trades['shares'], bins=np.arange(0, 1025, 25)) # percent of trades
plt.bar(np.arange(len(h)), h/len(trades['shares']), align='edge', width=-.5, edgecolor='white')
h,x = np.histogram(hidden_trades['shares'], bins=np.arange(0, 1025, 25))
plt.bar(np.arange(len(h)), h/len(hidden_trades['shares']), align='edge', width=.5, edgecolor='white', color='C3')
# plt.grid(True, linestyle='dashed', axis='y')
plt.legend(['Displayed', 'Hidden'])
plt.xticks(np.arange(0, 41, 4), np.arange(0, 1025, 100))


"""Single analysis"""
# books = pd.read_csv('/Volumes/datasets/ITCH/csv/111413/books/books_A.txt')
# messages = pd.read_csv('/Volumes/datasets/ITCH/csv/111413/messages/messages_A.txt')
# hidden = pd.read_csv('/Volumes/datasets/ITCH/csv/111413/trades/trades_A.txt')
# books, messages = nodups(books, messages)
# books['time'] = books['sec'] + books['nano'] / 10 ** 9
# messages['time'] = messages['sec'] + messages['nano'] / 10 ** 9
# hidden['time'] = hidden['sec'] + hidden['nano'] / 10 ** 9
# books = books[(books['time'] > 34200) & (books['time'] < 57000)]
# messages = messages[(messages['time'] > 34200) & (messages['time'] < 57000)]
# hidden = hidden[(hidden['time'] > 34200) & (hidden['time'] < 57000)]
#
# # Basics
# plt.plot(books['time'], books['bidprc0'])
# plt.plot(books['time'], books['askprc0'])
#
# books['bidvol0'].describe()
# books['askvol0'].describe()
#
# spread = books['askprc0'] - books['bidprc0']
# spread.describe()
# plt.plot(books['time'], spread)
#
# midprice = (books['bidprc0'] + books['askprc0']) / 2
# midprice.describe()
#
# messages.loc[:,('shares', 'price')].describe()
# pd.value_counts(messages['type'])
# pd.value_counts(messages['mpid'])
#
#
# # Trades
# trades = find_trades(messages)
# pd.value_counts(trades['side'])
# pd.value_counts(trades['hit'])
# trades.loc[:,('shares', 'vwap', 'hit')].describe()
# buys = trades[trades['side'] == 'B']
# sells = trades[trades['side'] == 'S']
# plt.scatter(buys['time'], buys['vwap'], alpha=0.50, s=8, color='C4')
# plt.scatter(sells['time'], sells['vwap'], alpha=0.50, s=8, color='C2')
# plt.legend(['buys', 'sells'])
# plt.bar(buys['time'], -buys['shares'], alpha=0.5, color='C0', width=10)
# plt.bar(sells['time'], sells['shares'], alpha=0.5, color='C1', width=10)
# plt.legend(['buys', 'sells'])
#
# # Hidden
# buys = hidden[hidden['side'] == 'S']
# sells = hidden[hidden['side'] == 'B']
# plt.scatter(buys['time'], buys['price'], alpha=0.25, s=8)
# plt.scatter(sells['time'], sells['price'], alpha=0.25, s=8)
# hidden.loc[:,('shares', 'price')].describe()
# pd.value_counts(hidden['side'])
# hidden_trades = find_trades(hidden)
# hidden_trades.loc[:,('shares', 'price', 'hit')].describe()
