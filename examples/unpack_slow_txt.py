import hfttools as hft
import pandas as pd
import datetime
import sys
import time

ver = 4.1
nlevels = 5
columns = ['sec', 'nano', 'name']
columns.extend(['bidprc{}'.format(i) for i in range(nlevels)])
columns.extend(['askprc{}'.format(i) for i in range(nlevels)])
columns.extend(['bidvol{}'.format(i) for i in range(nlevels)])
columns.extend(['askvol{}'.format(i) for i in range(nlevels)])
names = pd.read_csv('/Users/colinswaney/Desktop/SP500.txt')['Symbol']
names = [name.lstrip(' ') for name in names]
system_path = '/Users/colinswaney/Desktop/system/'
messages_path = '/Users/colinswaney/Desktop/messages/'
books_path = '/Users/colinswaney/Desktop/books/'
trades_path = '/Users/colinswaney/Desktop/trades/'
noii_path = '/Users/colinswaney/Desktop/noii/'
with open(system_path + 'system.txt', 'w') as system_file:
    system_file.write('sec,nano,name,event\n')
for name in names:
    with open(messages_path + 'messages_{}.txt'.format(name), 'w') as messages_file:
        messages_file.write('sec,nano,name,type,refno,side,shares,price,mpid\n')
    with open(books_path + 'books_{}.txt'.format(name), 'w') as books_file:
        books_file.write(','.join(columns) + '\n')
    with open(trades_path + 'trades_{}.txt'.format(name), 'w') as trades_file:
        trades_file.write('sec,nano,name,side,shares,price\n')
    with open(noii_path + 'noii_{}.txt'.format(name), 'w') as noii_file:
        noii_file.write('sec,nano,name,type,cross,shares,price,paired,imb,dir,far,near,curr\n')

# Setup for data processing
date = '070113'
fin = '/Volumes/datasets/ITCH/bin/S{}-v41.txt'.format(date)
data = open(fin, 'rb')
books = {}
for name in names:
    books[name] = hft.Book(date, name, nlevels)
orderlist = hft.Orderlist()
message_count = 0
clock = 0
reading = True
start = time.time()
while reading:

    # parse message
    message_size = hft.get_message_size(data.read(2))
    message_type = hft.get_message_type(data.read(1))
    message_bytes = data.read(message_size - 1)
    message = hft.get_message(message_bytes, message_type, date, clock, ver)
    message_count += 1

    # timestamps
    if message_type == 'T':
        if message.sec % 1800 == 0:
            print('TIME={}'.format(message.sec))
        clock = message.sec

    # system messages
    if message_type == 'S':
        print('SYSTEM MESSAGE: {}'.format(message.event))
        message.to_txt(system_path + 'system.txt')
        if message.event == 'O':  # start messages
            pass
        elif message.event == 'S':  # start system
            pass
        elif message.event == 'Q':  # start market hours
            pass
        elif message.event == 'A':  # trading halt
            pass
        elif message.event == 'R':  # quote only
            pass
        elif message.event == 'B':  # resume trading
            pass
        elif message.event == 'M':  # end market
            pass
        elif message.event == 'E':  # end system
            pass
        elif message.event == 'C':  # end messages
            reading = False
    elif message_type == 'H':
        if message.name in names:
            print('TRADING MESSAGE ({}): {}'.format(message.name, message.event))
            message.to_txt(system_path + 'system.txt')
            if message.event == 'H':  # halted (all US)
                pass
            elif message.event == 'P':  # paused (all US)
                pass
            elif message.event == 'Q':  # quotation only
                pass
            elif message.event == 'T':  # trading on nasdaq
                pass

    # market messages
    if message_type == 'U':
        del_message, add_message = message.split()
        orderlist.complete_message(del_message)
        orderlist.complete_message(add_message)
        if del_message.name in names:
            # print('{}'.format(del_message))
            orderlist.update(del_message)
            books[del_message.name].update(del_message)
            books[del_message.name].to_txt(books_path + 'books_{}.txt'.format(del_message.name))
            del_message.to_txt(messages_path + 'messages_{}.txt'.format(del_message.name))
            # print('{}'.format(add_message))
            orderlist.add(add_message)
            books[add_message.name].update(add_message)
            books[add_message.name].to_txt(books_path + 'books_{}.txt'.format(add_message.name))
            add_message.to_txt(messages_path + 'messages_{}.txt'.format(add_message.name))
    elif message_type in ('E', 'C', 'X', 'D'):
        orderlist.complete_message(message)
        if message.name in names:
            # print('{}'.format(message))
            orderlist.update(message)
            books[message.name].update(message)
            books[message.name].to_txt(books_path + 'books_{}.txt'.format(message.name))
            message.to_txt(messages_path + 'messages_{}.txt'.format(message.name))
    elif message_type in ('A', 'F'):
        if message.name in names:
            # print('{}'.format(message))
            orderlist.add(message)
            books[message.name].update(message)
            books[message.name].to_txt(books_path + 'books_{}.txt'.format(message.name))
            message.to_txt(messages_path + 'messages_{}.txt'.format(message.name))
    elif message_type in ('P'):
        if message.name in names:
            # print('{}'.format(message))
            message.to_txt(trades_path + 'trades_{}.txt'.format(message.name))

    # noii messages
    if message_type == 'Q':
        if message.name in names:
            print('CROSS ({}): {} @ {}'.format(message.name, message.shares, message.price))
            message.to_txt(noii_path + 'noii_{}.txt'.format(message.name))
    if message_type == 'I':
        if message.name in names:
            print('NOII: {}'.format(message))
            message.to_txt(noii_path + 'noii_{}.txt'.format(message.name))

stop = time.time()
print('Total messages: {}'.format(message_count))
print('Elapsed time: {:.2f}'.format(stop - start))
