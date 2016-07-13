
import pandas as pd
import numpy as np
import itch
import h5py
import sys
import time as timer

## CONSTANTS ##
DATE = sys.argv[1]
LEVELS = int(sys.argv[2])
MROWS = 10**1                                                                   ###
BROWS = 10**1

## DATA ##
fin = open('/Users/colinswaney/Research/Data/ITCH/bin/S' + DATE + '-v41.bin', 'rb')
# fout = h5py.File('/Users/colinswaney/Research/Data/ITCH/hdf5/hft.hdf5', 'a')  ###
fout = h5py.File('/Users/colinswaney/Research/Data/ITCH/hdf5/test.hdf5', 'a')   ###
time = 0  # time (sec)
writing = 'OFF'  # turns ON at 9:30 AM (Q)
reading = 'ON'  # turns OFF at 4:00 PM (M)
orderlist = itch.Orderlist()  # key: name, value: itch.Order
namelist = itch.import_names('names.txt')  # tickers
namelist = ['GOOG']                                                             ###
booklist = itch.Booklist(namelist, LEVELS)  # key: name, value: itch.Book
messagecounts = {}  # total writes to messagedata
bookcounts = {}  # total writes to bookdata
messagetempcounts = {}  # subtotal writes to messagedata
booktempcounts = {}  # subtotal writes to bookdata
messagedata = {}  # key: name, value: pd.DataFrame
bookdata = {}  # key: name, value: pdDataFrame
for name in namelist:
    messagedata[name] = np.empty((MROWS,9))
    messagedata[name].fill(0)
    messagecounts[name] = 0
    messagetempcounts[name] = 0
    bookdata[name] = np.empty((BROWS,2 + LEVELS * 4))
    bookdata[name].fill(0)
    bookcounts[name] = 0
    booktempcounts[name] = 0
input('PAUSED for memory check...')

## WHILE-LOOP ##
message_count = 0
start = timer.time()
while reading == 'ON':

    # read message...
    message_size = itch.get_message_size(fin.read(2))  # read 2 bytes
    message_type = itch.get_message_type(fin.read(1))  # read 1 bytes
    message_bytes = fin.read(message_size - 1)
    message = itch.get_message(message_bytes, message_type, time)
    message_count += 1

    # update time...
    if message_type == 'T':
        # print('time=', time, sep='')
        time = message.sec
    # update system...
    if message_type == 'S':
        print('SYSTEM MESSAGE:', message.event)
        if message.event == 'Q':
            writing = 'ON'
        elif message.event == 'M':
            reading = 'OFF'

    # complete message...
    if message_type == 'U':
        delete_message, replace_message = message.split()
        orderlist.complete_message(delete_message)
        orderlist.complete_message(replace_message)
    elif message_type in ('E', 'C', 'X', 'D'):
        orderlist.complete_message(message)

    # update orderlist...
    if message_type == 'U':
        if delete_message.name in namelist:
            orderlist.update(delete_message)
        if replace_message.name in namelist:
            orderlist.add_order(replace_message)
            if delete_message.name == 'GOOG':                                   ###
                print('replace message!')
                print(delete_message)
                print(replace_message)
    elif message_type in ('E', 'C', 'X', 'D'):
        if message.name in namelist:
            orderlist.update(message)
            if message.name == 'GOOG':                                          ###
                print(message)
    elif message_type in ('A', 'F'):
        if message.name in namelist:
            orderlist.add_order(message)
            if message.name == 'GOOG':                                          ###
                print(message)

    # update booklist...
    if message_type == 'U':
        if delete_message.name in namelist:
            booklist.books[delete_message.name].update(delete_message)
        if replace_message.name in namelist:
            booklist.books[replace_message.name].update(replace_message)
    elif message_type in ('A', 'F', 'E', 'C', 'X', 'D'):
        if message.name in namelist:
            booklist.books[message.name].update(message)

    # update messagedata...
    if writing == 'ON':
        if message_type in ('A', 'F', 'E', 'C', 'X', 'D'):
            if message.name in namelist:
                # print('message written to messagedata:', message.type)
                row = messagetempcounts[message.name]
                messagedata[message.name][row,:] = message.values()
                messagecounts[message.name] += 1
                messagetempcounts[message.name] += 1
                # print('messagecounts:', messagecounts[message.name])
                # print('messagetempcounts:', messagetempcounts[message.name])
        elif message_type == 'U':
            if delete_message.name in namelist:
                # print('message written to messagedata:', delete_message.type)
                row = messagetempcounts[delete_message.name]
                messagedata[delete_message.name][row,:] = delete_message.values()
                messagecounts[delete_message.name] += 1
                messagetempcounts[delete_message.name] += 1
                # print('messagecounts:', messagecounts[delete_message.name])
                # print('messagetempcounts:', messagetempcounts[delete_message.name])
                # print('message written to messagedata:', replace_message.type)
                row = messagetempcounts[replace_message.name]
                messagedata[replace_message.name][row,:] = replace_message.values()
                messagecounts[replace_message.name] += 1
                messagetempcounts[replace_message.name] += 1
                # print('messagecounts:', messagecounts[replace_message.name])
                # print('messagetempcounts:', messagetempcounts[replace_message.name])
        else:
            pass

    # update bookdata...
    if writing == 'ON':
        if message_type in ('A', 'F', 'E', 'C', 'X', 'D'):
            if message.name in namelist:
                # print('message written to bookdata:', message_type)
                row = booktempcounts[message.name]
                book = booklist.books[message.name]
                bookdata[message.name][row,:] = book.values()
                bookcounts[message.name] += 1
                booktempcounts[message.name] += 1
                # print('bookcounts:', bookcounts[message.name])
                # print('booktempcounts:', booktempcounts[message.name])
                if message.name == 'GOOG':                                      ###
                    print(booklist.books['GOOG'])
                    # input('paused...')

        elif message_type == 'U':
            if delete_message.name in namelist:
                # print('message written to bookdata:', message_type)
                row = booktempcounts[delete_message.name]
                book = booklist.books[delete_message.name]
                bookdata[delete_message.name][row,:] = book.values()
                bookcounts[delete_message.name] += 1
                booktempcounts[delete_message.name] += 1
                # print('bookcounts:', bookcounts[delete_message.name])
                # print('booktempcounts:', booktempcounts[delete_message.name])
                if delete_message.name == 'GOOG':                               ###
                    print(booklist.books['GOOG'])
                    input('paused...')
        else:
            pass

    # messagedata to HDF5...
    if writing == 'ON':
        if message_type in ('A', 'F', 'E', 'C', 'X', 'D'):
            if message.name in namelist:
                if messagetempcounts[message.name] >= MROWS - 2:
                    print('messagedata written to HDF5:', message.name)
                    name = message.name
                    data = messagedata[name][0:messagetempcounts[name],:]
                    count = messagecounts[name]
                    group = 'messages'
                    if count <= MROWS:
                        initial_write = True
                    else:
                        initial_write = False
                    itch.writedata(data,
                                   fout,
                                   group,
                                   name,
                                   DATE,
                                   count,
                                   init=initial_write)
                    messagetempcounts[message.name] = 0
        elif message_type == 'U':
            if delete_message.name in namelist and replace_message.name in namelist:
                if messagetempcounts[delete_message.name] >= MROWS - 2:
                    print('messagedata written to HDF5:', delete_message.name)
                    name = delete_message.name
                    data = messagedata[name][0:messagetempcounts[name],:]
                    count = messagecounts[name]
                    group = 'messages'
                    if count <= MROWS:
                        initial_write = True
                    else:
                        initial_write = False
                    itch.writedata(data,
                                   fout,
                                   group,
                                   name,
                                   DATE,
                                   count,
                                   init=initial_write)
                    messagetempcounts[delete_message.name] = 0
        else:
            pass

    # bookdata to HDF5...
    if writing == 'ON':
        if message_type in ('A', 'F', 'E', 'C', 'X', 'D'):
            if message.name in namelist:
                if booktempcounts[message.name] == BROWS:
                    # input('PAUSED for memory check...')
                    print('bookdata written to HDF5:', message.name)
                    name = message.name
                    data = bookdata[name]
                    print(data)                                                 ###
                    count = bookcounts[name]
                    group = 'orderbooks'
                    if count == BROWS:
                        initial_write = True
                    else:
                        initial_write = False
                    itch.writedata(data,
                                   fout,
                                   group,
                                   name,
                                   DATE,
                                   count,
                                   init=initial_write)
                    booktempcounts[message.name] = 0
        elif message_type == 'U':
            if delete_message.name in namelist and replace_message.name in namelist:
                if booktempcounts[delete_message.name] == BROWS:
                    # input('PAUSED for memory check...')
                    print('bookdata written to HDF5 (U):', delete_message.name)
                    name = delete_message.name
                    data = bookdata[name]
                    print(data)                                                 ###
                    count = bookcounts[name]
                    group = 'orderbooks'
                    if count == BROWS:
                        initial_write = True
                    else:
                        initial_write = False
                    itch.writedata(data,
                                   fout,
                                   group,
                                   name,
                                   DATE,
                                   count,
                                   init=initial_write)
                    booktempcounts[delete_message.name] = 0
        else:
            pass

stop = timer.time()
input('PAUSED for memory check...')

## CLEAN-UP ##
for name in namelist:
    data = messagedata[name][0:messagetempcounts[name],:]
    count = messagecounts[name]
    group = 'messages'
    print('final message write')
    itch.writedata(data,
               fout,
               group,
               name,
               DATE,
               count,
               init=False)
    data = bookdata[name][0:booktempcounts[name],:]
    count = bookcounts[name]
    group = 'orderbooks'
    print('final book write')
    itch.writedata(data,
                   fout,
                   group,
                   name,
                   DATE,
                   count,
                   init=False)
fin.close()
fout.close()

## OUTPUT ##
print('Elapsed time:', stop - start, 'sec')
print('Messages read:', message_count)
