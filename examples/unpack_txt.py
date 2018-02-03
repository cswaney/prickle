import hfttools as hft
import pandas as pd
import datetime
import sys
import os
import time

ver = 4.1
_, date = sys.argv

root = '/Volumes/datasets/ITCH/'
if os.path.exists('{}/csv/{}/'.format(root, date)):
    response = input('A directory already exists for the date provided. Are you sure you want to proceed? [Y/N] ')
    if response == 'Y':
        proceed = True
        for item in os.listdir('{}/csv/{}/system/'.format(root, date)):
            os.remove('{}/csv/{}/system/{}'.format(root, date, item))
        for item in os.listdir('{}/csv/{}/messages/'.format(root, date)):
            os.remove('{}/csv/{}/messages/{}'.format(root, date, item))
        for item in os.listdir('{}/csv/{}/books/'.format(root, date)):
            os.remove('{}/csv/{}/books/{}'.format(root, date, item))
        for item in os.listdir('{}/csv/{}/trades/'.format(root, date)):
            os.remove('{}/csv/{}/trades/{}'.format(root, date, item))
        for item in os.listdir('{}/csv/{}/noii/'.format(root, date)):
            os.remove('{}/csv/{}/noii/{}'.format(root, date, item))
        os.rmdir('/Volumes/datasets/ITCH/csv/{}/system/'.format(date))
        os.rmdir('/Volumes/datasets/ITCH/csv/{}/messages/'.format(date))
        os.rmdir('/Volumes/datasets/ITCH/csv/{}/books/'.format(date))
        os.rmdir('/Volumes/datasets/ITCH/csv/{}/trades/'.format(date))
        os.rmdir('/Volumes/datasets/ITCH/csv/{}/noii/'.format(date))
        os.rmdir('/Volumes/datasets/ITCH/csv/{}/'.format(date))
else:
    proceed = True

if proceed:
    print('Creating a database in directory: {}/csv/{}/'.format(root, date))

    # Paths
    main_path = '{}/csv/{}/'.format(root, date)
    system_path = '{}/csv/{}/system/'.format(root, date)
    messages_path = '{}/csv/{}/messages/'.format(root, date)
    books_path = '{}/csv/{}/books/'.format(date)
    trades_path = '{}/csv/{}/trades/'.format(date)
    noii_path = '{}/csv/{}/noii/'.format(date)

    # Directories
    os.makedirs(main_path)
    os.makedirs(system_path)
    os.makedirs(messages_path)
    os.makedirs(books_path)
    os.makedirs(trades_path)
    os.makedirs(noii_path)

    # Files
    nlevels = 5
    columns = ['sec', 'nano', 'name']
    columns.extend(['bidprc{}'.format(i) for i in range(nlevels)])
    columns.extend(['askprc{}'.format(i) for i in range(nlevels)])
    columns.extend(['bidvol{}'.format(i) for i in range(nlevels)])
    columns.extend(['askvol{}'.format(i) for i in range(nlevels)])
    names = pd.read_csv('{}/SP500.txt'.format(root))['Symbol']
    names = [name.lstrip(' ') for name in names]
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

    # Setup
    fin = '{}/bin/S{}-v41.txt'.format(root, date)
    data = open(fin, 'rb')
    books = {}
    books_buffer = {}
    messages_buffer = {}
    trades_buffer = {}
    for name in names:
        books[name] = hft.Book(date, name, nlevels)
        books_buffer[name] = []
        messages_buffer[name] = []
        trades_buffer[name] = []
    orderlist = hft.Orderlist()
    message_count = 0
    buffer_size = 10000
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
                # print('TRADING MESSAGE ({}): {}'.format(message.name, message.event))
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
            message, del_message, add_message = message.split()
            orderlist.complete_message(message)
            orderlist.complete_message(del_message)
            orderlist.complete_message(add_message)
            if message.name in names:
                orderlist.update(del_message)
                books[del_message.name].update(del_message)
                orderlist.add(add_message)
                books[add_message.name].update(add_message)
        elif message_type in ('E', 'C', 'X', 'D'):
            orderlist.complete_message(message)
            if message.name in names:
                orderlist.update(message)
                books[message.name].update(message)
        elif message_type in ('A', 'F'):
            if message.name in names:
                orderlist.add(message)
                books[message.name].update(message)
        elif message_type in ('P'):
            if message.name in names:
                pass

        if message_type in ('U', 'E', 'C', 'X', 'D', 'A', 'F'):
            if message.name in names:
                books_buffer[message.name].append(books[message.name].to_txt())
                if len(books_buffer[message.name]) == buffer_size:
                    with open('{}/books_{}.txt'.format(books_path, message.name), 'a') as fout:
                        fout.writelines(books_buffer[message.name])
                        # print('WROTE {} lines to books_{}.txt'.format(buffer_size, message.name))
                    books_buffer[message.name] = []
                messages_buffer[message.name].append(message.to_txt())
                if len(messages_buffer[message.name]) == buffer_size:
                    with open('{}/messages_{}.txt'.format(messages_path, message.name), 'a') as fout:
                        fout.writelines(messages_buffer[message.name])
                        # print('WROTE {} lines to messages_{}.txt'.format(buffer_size, message.name))
                    messages_buffer[message.name] = []
        if message_type in ('P'):
            if message.name in names:
                trades_buffer[message.name].append(message.to_txt())
                if len(trades_buffer[message.name]) == buffer_size:
                    with open(trades_path + 'trades_{}.txt'.format(message.name), 'a') as fout:
                        fout.writelines(trades_buffer[message.name])
                        # print('WROTE {} lines to trades_{}.txt'.format(buffer_size, message.name))
                    trades_buffer[message.name] = []

        # noii messages
        if message_type == 'Q':
            if message.name in names:
                # print('CROSS ({}): {} @ {}'.format(message.name, message.shares, message.price))
                message.to_txt(noii_path + 'noii_{}.txt'.format(message.name))
        if message_type == 'I':
            if message.name in names:
                # print('NOII: {}'.format(message))
                message.to_txt(noii_path + 'noii_{}.txt'.format(message.name))

    print('Wrapping up...')
    for name in names:
        with open('{}/books_{}.txt'.format(books_path, name), 'a') as fout:
            fout.writelines(books_buffer[name])
            lines = len(books_buffer[name])
            # print('WROTE {} lines to books_{}.txt'.format(lines, name))
        with open('{}/messages_{}.txt'.format(messages_path, name), 'a') as fout:
            fout.writelines(messages_buffer[name])
            lines = len(messages_buffer[name])
            # print('WROTE {} lines to messages_{}.txt'.format(lines, name))
        with open('{}/trades_{}.txt'.format(trades_path, name), 'a') as fout:
            fout.writelines(trades_buffer[name])
            lines = len(trades_buffer[name])
            # print('WROTE {} lines to trades_{}.txt'.format(lines, name))

    stop = time.time()
    print('Total messages: {}'.format(message_count))
    print('Elapsed time: {:.2f}'.format(stop - start))
