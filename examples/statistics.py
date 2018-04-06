from prickle import nodups, find_trades
import pandas as pd
import numpy as np
import os
import time

root = '/Volumes/datasets/ITCH/'
dates = [date for date in os.listdir('{}/csv/'.format(root)) if date != '.DS_Store']
names = [name.lstrip(' ') for name in pd.read_csv('{}/SP500.txt'.format(root))['Symbol']]

# message_counts.txt
output = []
for name in sorted(names):
    for date in dates[:-1]:
        start = time.time()
        messages = pd.read_csv('{}/csv/{}/messages/messages_{}.txt'.format(root, date, name))
        books = pd.read_csv('{}/csv/{}/books/books_{}.txt'.format(root, date, name))
        messages['time'] = messages['sec'] + messages['nano'] / 10 ** 9
        messages = messages[(messages['time'] > 34200) & (messages['time'] < 57600)]
        books['time'] = books['sec'] + books['nano'] / 10 ** 9
        books = books[(books['time'] > 34200) & (books['time'] < 57600)]
        books, messages = nodups(books, messages)
        counts = pd.value_counts(messages['type']).sort_index()
        row = [date, name] + list(counts)
        output.append(row)
        stop = time.time()
        print('Processing data for {}, {} (time={})'.format(name, date, stop - start))
df = pd.DataFrame(output, columns=['date', 'name', 'A', 'C', 'D', 'E', 'F', 'U', 'X'])
df.to_csv('/Volumes/datasets/ITCH/stats/message_counts.txt')

# message_shares.txt
output = []
for name in sorted(names):
    for date in dates[:-1]:
        start = time.time()
        messages = pd.read_csv('{}/csv/{}/messages/messages_{}.txt'.format(root, date, name))
        books = pd.read_csv('{}/csv/{}/books/books_{}.txt'.format(root, date, name))
        messages['time'] = messages['sec'] + messages['nano'] / 10 ** 9
        messages = messages[(messages['time'] > 34200) & (messages['time'] < 57600)]
        books['time'] = books['sec'] + books['nano'] / 10 ** 9
        books = books[(books['time'] > 34200) & (books['time'] < 57600)]
        books, messages = nodups(books, messages)
        for label in ['A', 'C', 'D', 'E', 'F', 'U', 'X']:
            shares = np.abs(messages[messages['type'] == label]['shares'])
            cnts, bins = np.histogram(shares, np.arange(0, 2025, 25))
            output.append([date, name, label] + list(cnts))
        stop = time.time()
        print('Processing data for {}, {} (time={})'.format(name, date, stop - start))
df = pd.DataFrame(output, columns=['date', 'name', 'type'] + list(np.arange(0, 2000, 25)))
df.to_csv('/Volumes/datasets/ITCH/stats/message_shares.txt')

# message_times.txt
output = []
for name in sorted(names):
    for date in dates[:-1]:
        start = time.time()
        messages = pd.read_csv('{}/csv/{}/messages/messages_{}.txt'.format(root, date, name))
        books = pd.read_csv('{}/csv/{}/books/books_{}.txt'.format(root, date, name))
        messages['time'] = messages['sec'] + messages['nano'] / 10 ** 9
        messages = messages[(messages['time'] > 34200) & (messages['time'] < 57600)]
        books['time'] = books['sec'] + books['nano'] / 10 ** 9
        books = books[(books['time'] > 34200) & (books['time'] < 57600)]
        books, messages = nodups(books, messages)
        for label in ['A', 'C', 'D', 'E', 'F', 'U', 'X']:
            times = messages[messages['type'] == label]['time']
            cnts, bins = np.histogram(times, np.arange(34200, 57900, 300))
            output.append([date, name, label] + list(cnts))
        stop = time.time()
        print('Processing data for {}, {} (time={})'.format(name, date, stop - start))
df = pd.DataFrame(output, columns=['date', 'name', 'type'] + list(np.arange(34200, 57600, 300)))
df.to_csv('/Volumes/datasets/ITCH/stats/message_times.txt')

# message_nano.txt
output = []
for name in sorted(names):
    for date in dates[:-1]:
        start = time.time()
        messages = pd.read_csv('{}/csv/{}/messages/messages_{}.txt'.format(root, date, name))
        books = pd.read_csv('{}/csv/{}/books/books_{}.txt'.format(root, date, name))
        messages['time'] = messages['sec'] + messages['nano'] / 10 ** 9
        messages = messages[(messages['time'] > 34200) & (messages['time'] < 57600)]
        books['time'] = books['sec'] + books['nano'] / 10 ** 9
        books = books[(books['time'] > 34200) & (books['time'] < 57600)]
        books, messages = nodups(books, messages)
        for label in ['A', 'C', 'D', 'E', 'F', 'U', 'X']:
            nanos = messages[messages['type'] == label]['nano']
            cnts, bins = np.histogram(nanos, bins=np.arange(0, 10 ** 9 + 2 * 10 ** 7, 2 * 10 ** 7))
            output.append([date, name, label] + list(cnts))
        stop = time.time()
        print('Processing data for {}, {} (time={})'.format(name, date, stop - start))
df = pd.DataFrame(output, columns=['date', 'name', 'type'] + list(np.arange(0, 10 ** 9, 2 * 10 ** 7)))
df.to_csv('/Volumes/datasets/ITCH/stats/message_nano.txt')

# trades.txt
output = []
for name in sorted(names):
    for date in dates[:-1]:
        start = time.time()
        messages = pd.read_csv('{}/csv/{}/messages/messages_{}.txt'.format(root, date, name))
        messages['time'] = messages['sec'] + messages['nano'] / 10 ** 9
        messages = messages[(messages['time'] > 34200) & (messages['time'] < 57600)]
        trades = find_trades(messages)
        trades['date'] = date
        trades['name'] = name
        output.append(trades)
        stop = time.time()
        print('Processing data for {}, {} (time={})'.format(name, date, stop - start))
df = pd.concat(output)
df.to_csv('/Volumes/datasets/ITCH/stats/trades.txt')

# hidden.txt
output = []
for name in sorted(names):
    for date in dates[:-1]:
        start = time.time()
        hidden = pd.read_csv('{}/csv/{}/trades/trades_{}.txt'.format(root, date, name))
        hidden['time'] = hidden['sec'] + hidden['nano'] / 10 ** 9
        hidden = hidden[(hidden['time'] > 34200) & (hidden['time'] < 57000)]
        trades = find_trades(hidden)
        trades['date'] = date
        trades['name'] = name
        output.append(trades)
        stop = time.time()
        print('Processing data for {}, {} (time={})'.format(name, date, stop - start))
df = pd.concat(output)
df.to_csv('/Volumes/datasets/ITCH/stats/hidden_trades.txt')
