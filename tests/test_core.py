import numpy as np
import h5py
import hfttools as hft
import time

maxrows = 10**4
nlevels = 50
date = '2013-03-01'
path = '/Users/colinswaney/Desktop'

namelist = ['GOOG', 'AAPL']
orderlist = hft.Orderlist()
booklist = hft.Booklist(namelist, nlevels)
messagelist = hft.Messagelist(namelist)

db = hft.Database(path + '/sample-itch.hdf5', namelist)
data = open(path + '/sample-itch.bin', 'rb')

messagecount = 0

reading = True
writing = False
clock = 0

start = time.time()

while reading:

    # read message
    message_size = hft.get_message_size(data.read(2))
    message_type = hft.get_message_type(data.read(1))
    message_bytes = data.read(message_size - 1)
    message = hft.get_message(message_bytes, message_type, clock)
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
        input('PAUSED (press any button to continue).')

    # complete message
    if message_type == 'U':
        delete_message, add_message = message.split()
        orderlist.complete_message(delete_message)
        orderlist.complete_message(add_message)
    elif message_type in ('E', 'C', 'X', 'D'):
        orderlist.complete_message(message)

    # update orders
    if message_type == 'U':
        if delete_message.name in namelist:
            orderlist.update(delete_message)
            # print(delete_message)
        if add_message.name in namelist:
            orderlist.add(add_message)
            # print(add_message)
    elif message_type in ('E', 'C', 'X', 'D'):
        if message.name in namelist:
            orderlist.update(message)
            # print(message)
    elif message_type in ('A', 'F'):
        if message.name in namelist:
            orderlist.add(message)
            # print(message)

    # update messages, books, and write to disk
    if message_type == 'U':
        if delete_message.name in namelist:
            # update messages
            messagelist.add(delete_message)
            # print('{} message added to messagelist'.format(delete_message.type))
            if len(messagelist.messages[delete_message.name]) == maxrows:
                messagelist.to_hdf5(name=delete_message.name, db=db)
                # input('Press any button to continue.')
            # update books
            booklist.update(delete_message)
            # print('{} book was updated.'.format(delete_message.name))
            if len(booklist.books[delete_message.name]['hist']) == maxrows:
                booklist.to_hdf5(name=delete_message.name, db=db)
                # input('Press any button to continue.')
        if add_message.name in namelist:
            # update messages
            messagelist.add(add_message)
            # print('{} message added to messagelist'.format(add_message.type))
            if len(messagelist.messages[add_message.name]) == maxrows:
                messagelist.to_hdf5(name=add_message.name, db=db)
                # input('Press any button to continue.')
            # update books
            booklist.update(add_message)
            # print('{} book was updated.'.format(add_message.name))
            if len(booklist.books[add_message.name]['hist']) == maxrows:
                booklist.to_hdf5(name=add_message.name, db=db)
                # input('Press any button to continue.')
    elif message_type in ('A', 'F', 'E', 'C', 'X', 'D'):
        if message.name in namelist:
            # update messages
            messagelist.add(message)
            # print('{} message added to messagelist'.format(message.type))
            if len(messagelist.messages[message.name]) == maxrows:
                messagelist.to_hdf5(name=message.name, db=db)
                # input('Press any button to continue.')
            # update books
            booklist.update(message)
            # print('{} book was updated.'.format(message.name))
            if len(booklist.books[message.name]['hist']) == maxrows:
                booklist.to_hdf5(name=message.name, db=db)
                # input('Press any button to continue.')

# clean up
for name in namelist:
    messagelist.to_hdf5(name=name, db=db)
    booklist.to_hdf5(name=name, db=db)

stop = time.time()

data.close()
db.close()

print('Elapsed time: {} seconds'.format(stop - start))
print('Messages read: {}'.format(messagecount))
