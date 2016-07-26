import hfttools as hft
import pandas as pd
import numpy as np
import psycopg2
import h5py
import time

def unpack(fin, ver, date, fout, nlevels, names, method=None):
    """Method to unpack a ITCH data file and reconstruct the limit order book."""

    MAXROWS = 10**4

    orderlist = hft.Orderlist()
    booklist = hft.Booklist(names, nlevels)
    messagelist = hft.Messagelist(names)

    if method == 'hdf5':
        db = hft.Database(fout, names)
    elif method == 'postgres':
        db = hft.Postgres(host='localhost', user='colinswaney', nlevels=nlevels)
    elif method == 'csv':
        pass
    else:
        print('No database option specified. Creating comma-seperated value files.')
    data = open(fin, 'rb')

    messagecount = 0

    reading = True
    writing = False
    clock = 0

    start = time.time()

    # unpacking
    while reading:

        # read message
        message_size = hft.get_message_size(data.read(2))
        message_type = hft.get_message_type(data.read(1))
        message_bytes = data.read(message_size - 1)
        message = hft.get_message(message_bytes, message_type, clock, version)
        messagecount += 1

        # update clock
        if message_type == 'T':
            if message.sec % 60 == 0:
                print('TIME={}'.format(message.sec))
            clock = message.sec

        # update system
        if message_type == 'S':
            print('SYSTEM MESSAGE: {}'.format(message.event))
            if message.event == 'Q':  # start system
                writing = True
            elif message.event == 'M':  # end market
                reading = False
            elif message.event == 'A':
                pass  # trading halt
            elif message.event == 'R':
                pass  # quote only
            elif message.event == 'B':
                pass  # resume trading
            input('PAUSED (press any button to continue).')

        if message_type == 'H':
            print('TRADE-ACTION MESSAGE: {}'.format(message.event))
            if message.event in ('H','P','V'):
                pass  # remove message.name from names
            elif message.event == 'T':
                pass  # add message.name to names (check that it isn't there already)
            elif message.event in ('Q','R'):
                pass  # quote only (only accepting A, F, X, D, U)

        # complete message
        if message_type == 'U':
            delete_message, add_message = message.split()
            orderlist.complete_message(delete_message)
            orderlist.complete_message(add_message)
        elif message_type in ('E', 'C', 'X', 'D'):
            orderlist.complete_message(message)
        print('completed the message.')

        # update orders
        if message_type == 'U':
            if delete_message.name in names:
                orderlist.update(delete_message)
                print(delete_message)
            if add_message.name in names:
                orderlist.add(add_message)
                print(add_message)
        elif message_type in ('E', 'C', 'X', 'D'):
            if message.name in names:
                orderlist.update(message)
                print(message)
        elif message_type in ('A', 'F'):
            if message.name in names:
                orderlist.add(message)
                print(message)
        print('updated the order list')

        # update messages, books, and write to disk
        if method == 'hdf5':
            if message_type == 'U':
                if delete_message.name in names:
                    # update messages
                    messagelist.add(delete_message)
                    print('{} message added to messagelist'.format(delete_message.type))
                    if len(messagelist.messages[delete_message.name]) == MAXROWS:
                            messagelist.to_hdf5(name=delete_message.name, db=db)
                        input('Press any button to continue.')
                    # update books
                    booklist.update(delete_message)
                    print('{} book was updated.'.format(delete_message.name))
                    if len(booklist.books[delete_message.name]['hist']) == MAXROWS:
                        booklist.to_hdf5(name=delete_message.name, db=db)
                        input('Press any button to continue.')
                if add_message.name in names:
                    # update messages
                    messagelist.add(add_message)
                    print('{} message added to messagelist'.format(add_message.type))
                    if len(messagelist.messages[add_message.name]) == MAXROWS:
                        messagelist.to_hdf5(name=add_message.name, db=db)
                        input('Press any button to continue.')
                    # update books
                    booklist.update(add_message)
                    print('{} book was updated.'.format(add_message.name))
                    if len(booklist.books[add_message.name]['hist']) == MAXROWS:
                        booklist.to_hdf5(name=add_message.name, db=db)
                        input('Press any button to continue.')
            elif message_type in ('A', 'F', 'E', 'C', 'X', 'D'):
                if message.name in names:
                    # update messages
                    messagelist.add(message)
                    print('{} message added to messagelist'.format(message.type))
                    if len(messagelist.messages[message.name]) == MAXROWS:
                        messagelist.to_hdf5(name=message.name, db=db)
                        input('Press any button to continue.')
                    # update books
                    booklist.update(message)
                    print('{} book was updated.'.format(message.name))
                    if len(booklist.books[message.name]['hist']) == MAXROWS:
                        booklist.to_hdf5(name=message.name, db=db)
                        input('Press any button to continue.')
        elif method == 'postgres':
            if message_type == 'U':
                if delete_message.name in namelist:
                    # update messages
                    messagelist.add(delete_message)
                    # print('{} message added to messagelist'.format(delete_message.type))
                    if len(messagelist.messages[delete_message.name]) == maxrows:
                        messagelist.to_postgres(date=date, name=delete_message.name, db=db)
                        # input('Press any button to continue.')
                    # update books
                    booklist.update(delete_message)
                    # print('{} book was updated.'.format(delete_message.name))
                    if len(booklist.books[delete_message.name]['hist']) == maxrows:
                        booklist.to_postgres(date=date, name=delete_message.name, db=db)
                        # input('Press any button to continue.')
                if add_message.name in namelist:
                    # update messages
                    messagelist.add(add_message)
                    # print('{} message added to messagelist'.format(add_message.type))
                    if len(messagelist.messages[add_message.name]) == maxrows:
                        messagelist.to_postgres(date=date, name=add_message.name, db=db)
                        # input('Press any button to continue.')
                    # update books
                    booklist.update(add_message)
                    # print('{} book was updated.'.format(add_message.name))
                    if len(booklist.books[add_message.name]['hist']) == maxrows:
                        booklist.to_postgres(date=date, name=add_message.name, db=db)
                        # input('Press any button to continue.')
            elif message_type in ('A', 'F', 'E', 'C', 'X', 'D'):
                if message.name in namelist:
                    # update messages
                    messagelist.add(message)
                    # print('{} message added to messagelist'.format(message.type))
                    if len(messagelist.messages[message.name]) == maxrows:
                        messagelist.to_postgres(date=date, name=message.name, db=db)
                        # input('Press any button to continue.')
                    # update books
                    booklist.update(message)
                    # print('{} book was updated.'.format(message.name))
                    if len(booklist.books[message.name]['hist']) == maxrows:
                        booklist.to_postgres(date=date, name=message.name, db=db)
                        # input('Press any button to continue.')
        elif method == 'csv':
            pass

    # clean up
    for name in names:
        messagelist.to_hdf5(name=name, db=db)
        booklist.to_hdf5(name=name, db=db)

    stop = time.time()

    data.close()
    db.close()

    print('Elapsed time: {} seconds'.format(stop - start))
    print('Messages read: {}'.format(messagecount))
