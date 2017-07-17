import hfttools as hft
import datetime
import sys

names = ['GOOG']
date = '070113'
fin = '/Volumes/datasets/ITCH/bin/S{}-v41.txt'.format(date)
ver = 4.1
nlevels = 10
method = 'hdf5'

data = open(fin, 'rb')
message_count = 0
reading = True
writing = False
clock = 0
orderlist = hft.Orderlist()

while True:

    message_size = hft.get_message_size(data.read(2))
    message_type = hft.get_message_type(data.read(1))
    message_bytes = data.read(message_size - 1)
    message = hft.get_message(message_bytes, message_type, date, clock, ver)

    # update clock
    if message_type == 'T':
        if message.sec % 60 == 0:
            print('TIME={}'.format(message.sec))
        clock = message.sec

    # update system
    if message_type == 'S':
        print('SYSTEM MESSAGE: {}'.format(message.event))
        if message.event == 'Q':  # start market
            writing = True
        elif message.event == 'M':  # end market
            reading = False
        elif message.event == 'A':
            pass  # trading halt
        elif message.event == 'R':
            pass  # quote only
        elif message.event == 'B':
            pass  # resume trading
        input()

    # complete message
    if message_type == 'U':
        del_message, add_message = message.split()
        orderlist.complete_message(del_message)
        orderlist.complete_message(add_message)
        if del_message.name in names:
            message_count += 1
            print('[{}] {}'.format(message_count, del_message))
            print('[{}] {}'.format(message_count, add_message))
            input()
    elif message_type in ('E', 'C', 'X', 'D'):
        orderlist.complete_message(message)
        if message.name in names:
            message_count += 1
            print('[{}] {}'.format(message_count, message))
            # input()
    elif message_type in ('A'):
        if message.name in names:
            message_count += 1
            orderlist.add(message)
            print('[{}] {}'.format(message_count, message))
            # input()
    elif message_type in ('F'):
        if message.name in names:
            message_count += 1
            orderlist.add(message)
            print('[{}] {}'.format(message_count, message))
            # input()
    elif message_type in ('P'):
        if message.name in names:
            message_count += 1
            print('[{}] {}'.format(message_count, message))
            input()
