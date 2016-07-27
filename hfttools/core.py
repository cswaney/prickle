import numpy as np
import pandas as pd
import psycopg2 as pg
import struct
import h5py
import time

class Database():
    """An HDF5 database to store message and order book data."""

    def __init__(self, path, names, nlevels):
        # open file, create otherwise
        try:
            self.file = h5py.File(path, 'r+')  # read/write, file must exist
            print('Overwriting existing HDF5 file.')
            for name in names:
                if name in self.file['messages'].keys():
                    del self.file['messages'][name]
                if name in self.file['orderbooks'].keys():
                    del self.file['orderbooks'][name]
        except OSError as e:
            print('HDF5 file does not exist. Creating a new one.')
            self.file = h5py.File(path, 'x')  # create file, fail if exists
        # open groups, create otherwise
        self.messages = self.file.require_group('messages')
        self.orderbooks = self.file.require_group('orderbooks')
        # open datasets, create otherwise
        for name in names:
            self.messages.require_dataset(name,
                                          shape=(0,9),
                                          maxshape=(None,None),
                                          dtype='i')
            self.orderbooks.require_dataset(name,
                                            shape=(0,4 * nlevels + 2),
                                            maxshape=(None,None),
                                            dtype='i')

    def close(self):
        self.file.close()

class Postgres():
    """A PostgreSQL database to store message and order book data. """

    def __init__(self, host, user, nlevels):

        self.host = host
        self.user = user
        self.nlevels = nlevels

        # create column info
        msg_sql = """create table messages (date varchar,
                                            sec integer,
                                            nano integer,
                                            type varchar,
                                            event varchar,
                                            name varchar,
                                            buysell varchar,
                                            price integer,
                                            shares integer,
                                            refno integer,
                                            newrefno integer)"""

        cols = ['date date',
                'sec integer',
                'nano integer',
                'name varchar']
        cols.extend(['bid_prc_' + str(i) + ' integer' for i in range(nlevels,0,-1)])
        cols.extend(['ask_prc_' + str(i) + ' integer' for i in range(1,nlevels+1)])
        cols.extend(['bid_vol_' + str(i) + ' integer' for i in range(nlevels,0,-1)])
        cols.extend(['ask_vol_' + str(i) + ' integer' for i in range(1,nlevels+1)])
        col_sql = ', '.join(cols)
        book_sql = 'create table orderbooks (' + col_sql + ')'

        # open connection to database
        try:
            conn = pg.connect(host=self.host, user=self.user)
            with conn.cursor() as cur:
                try:  # create message table
                    cur.execute(msg_sql)
                    conn.commit()
                except pg.Error as e:
                    print(e.pgerror)
                try:  # create orderbook table
                    cur.execute(book_sql)
                    conn.commit()
                except pg.Error as e:
                    print(e.pgerror)
            conn.close()
            print('Created a new PostgreSqL database.')
        except pg.Error as e:
            print('ERROR: unable to connect to database.')

    def open(self):
        self.conn = pg.connect(host=self.host, user=self.user)

    def close(self):
        self.conn.close()

class Message():
    """A class to represent messages.

    A class representing out-going messages from the NASDAQ system.

    Attributes:
        sec (int): seconds
        nano (int): nano seconds
        type (str): message type
        event (str): system event
        name (str): stock ticker
        buysell (str): buy or sell
        price (int): price
        shares (int): shares
        refno (int): reference number
        newrefno (int): replacement reference number

    """

    def __init__(self, sec=-1, nano=-1, type='.', event='.', name='.',
                 buysell='.', price=-1, shares=0, refno=-1, newrefno=-1):
        self.sec = sec
        self.nano = nano
        self.type = type
        self.event = event
        self.name = name
        self.buysell = buysell
        self.price = price
        self.shares = shares
        self.refno = refno
        self.newrefno = newrefno

    def __str__(self):
        sep = ', '
        line = ['sec=' + str(self.sec),
                'nano=' + str(self.nano),
                'type=' + str(self.type),
                'event=' + str(self.event),
                'name=' + str(self.name),
                'buysell=' + str(self.buysell),
                'price=' + str(self.price),
                'shares=' + str(self.shares),
                'refno=' + str(self.refno),
                'newrefno=' + str(self.newrefno)]
        return sep.join(line)

    def __repr__(self):
        sep = ', '
        line = ['sec: ' + str(self.sec),
                'nano: ' + str(self.nano),
                'type: ' + str(self.type),
                'event: ' + str(self.event),
                'name: ' + str(self.name),
                'buysell: ' + str(self.buysell),
                'price: ' + str(self.price),
                'shares: ' + str(self.shares),
                'refno: ' + str(self.refno),
                'newrefno: ' + str(self.newrefno)]
        return 'Message(' + sep.join(line) + ')'

    def split(self):
        """Converts a replace message to an add and a delete."""
        if self.type == 'U':
            del_message = Message(sec=self.sec, nano=self.nano, type='D',
                                     refno=self.refno, newrefno=-1)
            add_message = Message(sec=self.sec, nano=self.nano, type='U',
                                  price=self.price, shares=self.shares,
                                  refno=self.refno, newrefno=self.newrefno)
            return (del_message, add_message)
        else:
            print('Warning: "split" method called on non-replacement messages.')

    def to_list(self, date):
        """Returns message as a list."""

        values = []
        values.append(str(date))
        values.append(int(self.sec))
        values.append(int(self.nano))
        values.append(str(self.type))
        values.append(str(self.event))
        values.append(str(self.name))
        values.append(str(self.buysell))
        values.append(int(self.price))
        values.append(int(self.shares))
        values.append(int(self.refno))
        values.append(int(self.newrefno))
        return values

    def to_array(self):
        """Returns message as an np.array of integers."""

        values = []
        values.append(int(self.sec))
        values.append(int(self.nano))

        if self.type == 'T':  # timestamp
            values.append(0)
        elif self.type == 'S':  # system
            values.append(1)
        elif self.type in ('A', 'F'):  # adds
            values.append(2)
        elif self.type == 'X':  # cancel
            values.append(3)
        elif self.type == 'D':  # delete
            values.append(4)
        elif self.type == 'E':  # execute
            values.append(5)
        elif self.type == 'C':  # execute w/ price
            values.append(6)
        elif self.type == 'U':  # replace
            values.append(7)
        else:
            values.append(-1)  # other (ignored)

        if self.event == 'O':    # start messages
            values.append(0)
        elif self.event == 'S':  # start system hours
            values.append(1)
        elif self.event == 'Q':  # start market hours
            values.append(2)
        elif self.event == 'M':  # end market hours
            values.append(3)
        elif self.event == 'E':  # end system hours
            values.append(4)
        elif self.event == 'C':  # end messages
            values.append(5)
        elif self.event == 'A':  # halt trading
            values.append(6)
        elif self.event == 'R':  # quotes only
            values.append(7)
        elif self.event == 'B':  # resume trading
            values.append(8)
        else:
            values.append(-1)  # no event

        if self.buysell == 'B':  # bid
            values.append(1)
        elif self.buysell == 'S':  # ask
            values.append(-1)
        else:
            values.append(0)

        values.append(int(self.price))
        values.append(int(self.shares))
        values.append(int(self.refno))
        values.append(int(self.newrefno))

        return np.array(values)

class Messagelist():
    """A class to store messages.

    Provides methods for writing to HDF5 and PostgreSQL databases.

    Example:
        Create a MessageList::

            >> msglist = hft.Messagelist(['GOOG', 'AAPL'])

    Attributes:
        messages (list): list of Messages.

    """

    def __init__(self, names):
        self.messages = {}
        for name in names:
            self.messages[name] = []

    def add(self,message):
        """Add a message to the list."""
        try:
            self.messages[message.name].append(message)
        except KeyError as e:
            print("KeyError: Could not find {} in the message list".format(message.name))

    def to_hdf5(self, name, db):
        """Write messages to HDF5 file."""
        m = self.messages[name]
        listed = [message.to_array() for message in m]
        array = np.array(listed)
        db_size, db_cols = db.messages[name].shape  # rows
        array_size, array_cols = array.shape
        db_resize = db_size + array_size
        db.messages[name].resize((db_resize,db_cols))
        db.messages[name][db_size:db_resize,:] = array
        self.messages[name] = []  # reset
        # print('wrote {} lines to dataset {}'.format(array_size,
                                                    # db.messages[name]))

    def to_postgres(self, date, name, db):
        """Write messages to PostgreSQL database."""
        # connect to postgres
        db.open()
        # get the message list for name
        m = self.messages[name]
        # convert the messages to a list of lists
        listed = [message.to_list(date) for message in m]
        # write the messages/lists to the db
        with db.conn.cursor() as cursor:
            for message in listed:
                try:
                    cursor.execute('insert into messages values%s;', [tuple(message)])  # %s becomes "(x,..., x)"
                except pg.Error as e:
                    print(e.pgerror)
        db.conn.commit()
        db.close()
        self.messages[name] = [] # reset
        print('wrote {} lines to messages table for name {}'.format(len(m), name))

class Order():
    """A class to represent limit orders.

    Store message primary data for order book reconstruction.

    Attributes:
        name (str): stock ticker
        buysell (str): buy or sell
        price (int): price
        shares (int): shares

    """

    def __init__(self, name='.', buysell='.', price='.', shares='.'):
        self.name = name
        self.buysell = buysell
        self.price = price
        self.shares = shares

    def __str__(self):
        sep = ', '
        line = ['name=' + str(self.name),
                'buysell=' + str(self.buysell),
                'price=' + str(self.price),
                'shares=' + str(self.shares)]
        return sep.join(line)

    def __repr__(self):
        sep = ', '
        line = ['name=' + str(self.name),
                'buysell=' + str(self.buysell),
                'price=' + str(self.price),
                'shares=' + str(self.shares)]
        return 'Order(' + sep.join(line) + ')'

class Orderlist():
    """A class to store existing orders and process incoming messages.

    This class handles the matching of messages to standing orders. Incoming messages are first matched to standing orders so that missing message data can be completed, and then the referenced order is updated based on the message.

    Attributes:
        orders (dict): keys are reference numbers, values are Orders.

    """

    def __init__(self):
        self.orders = {}

    def __str__(self):
        sep = '\n'
        line = []
        for key in self.orders.keys():
            line.append(str(key) + ': ' + str(self.orders[key]))
        return sep.join(line)

    # updates message by reference.
    def complete_message(self, message):
        """Look up order for message and fill in missing data."""
        if message.refno in self.orders.keys():
            # print('complete_message received message: {}'.format(message.type))
            ref_order = self.orders[message.refno]
            if message.type == 'U':  # ADD from a split REPLACE order
                message.type = 'A'
                message.name = ref_order.name
                message.buysell = ref_order.buysell
                message.refno = message.newrefno
                message.newrefno = -1
            if message.type in ('E', 'C', 'X'):
                message.name = ref_order.name
                message.buysell = ref_order.buysell
                message.price = ref_order.price
                message.shares = -message.shares
            elif message.type == 'D':
                message.name = ref_order.name
                message.buysell = ref_order.buysell
                message.price = ref_order.price
                message.shares = -ref_order.shares

    def add(self, message):
        """Add a new order to the list."""
        order = Order()
        order.name = message.name
        order.buysell = message.buysell
        order.price = message.price
        order.shares = message.shares
        self.orders[message.refno] = order

    def update(self, message):
        """Update an existing order based on incoming message."""
        if message.refno in self.orders.keys():
            if message.type == 'E': # execute
                self.orders[message.refno].shares += message.shares
            elif message.type == 'X': # execute w/ price
                self.orders[message.refno].shares += message.shares
            elif message.type == 'C': # cancel
                self.orders[message.refno].shares += message.shares
            elif message.type == 'D': # delete
                self.orders.pop(message.refno)
        else:
            pass

class Book():
    """A class to represent an order book.

    This class provides a method for updating the state of an order book from an incoming message.

    Attributes:
        bids (dict): keys are prices, values are shares
        asks (dict): keys are prices, values are shares
        levels (int): levels of the the order book to track
        sec (int): seconds
        nano (int): nanoseconds

    """

    def __init__(self, levels):
        self.bids = {}
        self.asks = {}
        self.levels = levels
        self.sec = -1
        self.nano = -1

    def __str__(self):
        sep = ', '
        sorted_bids = sorted(self.bids.keys(), reverse=True)  # high-to-low
        sorted_asks = sorted(self.asks.keys())  # low-to-high
        bid_list = []
        ask_list = []
        nbids = len(self.bids)
        nasks = len(self.asks)
        for i in range(0, self.levels):
            if i < nbids:
                bid_list.append(str(self.bids[sorted_bids[i]]) + '@' + str(sorted_bids[i]))
            else:
                pass
            if i < nasks:
                ask_list.append(str(self.asks[sorted_asks[i]]) + '@' + str(sorted_asks[i]))
            else:
                pass
        return 'bids: ' + sep.join(bid_list) + '\n' + 'asks: ' + sep.join(ask_list)

    def __repr__(self):
        sep = ', '
        sorted_bids = sorted(self.bids.keys(), reverse=True)  # high-to-low
        sorted_asks = sorted(self.asks.keys())  # low-to-high
        bid_list = []
        ask_list = []
        nbids = len(self.bids)
        nasks = len(self.asks)
        for i in range(0, self.levels):
            if i < nbids:
                bid_list.append(str(self.bids[sorted_bids[i]]) + '@' + str(sorted_bids[i]))
            else:
                pass
            if i < nasks:
                ask_list.append(str(self.asks[sorted_asks[i]]) + '@' + str(sorted_asks[i]))
            else:
                pass
        return 'Book( \n' + 'bids: ' + sep.join(bid_list) + '\n' + 'asks: ' + sep.join(ask_list) + ' )'

    def update(self, message):
        """Update order book using incoming message data."""
        self.sec = message.sec
        self.nano = message.nano
        if message.buysell == 'B':
            if message.price in self.bids.keys():
                self.bids[message.price] += message.shares
                if self.bids[message.price] == 0:
                    self.bids.pop(message.price)
            else:
                if message.type in ('A','F'):
                    self.bids[message.price] = message.shares
        elif message.buysell == 'S':
            if message.price in self.asks.keys():
                self.asks[message.price] += message.shares
                if self.asks[message.price] == 0:
                    self.asks.pop(message.price)
            else:
                if message.type in ('A','F'):
                    self.asks[message.price] = message.shares
        return self

    def to_list(self, date, name):
        """Return order book as a list."""
        values = []
        values.append(date)
        values.append(int(self.sec))
        values.append(int(self.nano))
        values.append(name)
        sorted_bids = sorted(self.bids.keys(), reverse=True)
        sorted_asks = sorted(self.asks.keys())
        for i in range(0, self.levels): # bid price
            if i < len(self.bids):
                values.append(sorted_bids[i])
            else:
                values.append(0)
        for i in range(0, self.levels): # ask price
            if i < len(self.asks):
                values.append(sorted_asks[i])
            else:
                values.append(0)
        for i in range(0, self.levels): # bid depth
            if i < len(self.bids):
                values.append(self.bids[sorted_bids[i]])
            else:
                values.append(0)
        for i in range(0, self.levels): # ask depth
            if i < len(self.asks):
                values.append(self.asks[sorted_asks[i]])
            else:
                values.append(0)
        return values

    def to_array(self):
        '''Return order book as numpy array.'''
        values = []
        values.append(int(self.sec))
        values.append(int(self.nano))
        sorted_bids = sorted(self.bids.keys(), reverse=True)
        sorted_asks = sorted(self.asks.keys())
        for i in range(0, self.levels): # bid price
            if i < len(self.bids):
                values.append(sorted_bids[i])
            else:
                values.append(0)
        for i in range(0, self.levels): # ask price
            if i < len(self.asks):
                values.append(sorted_asks[i])
            else:
                values.append(0)
        for i in range(0, self.levels): # bid depth
            if i < len(self.bids):
                values.append(self.bids[sorted_bids[i]])
            else:
                values.append(0)
        for i in range(0, self.levels): # ask depth
            if i < len(self.asks):
                values.append(self.asks[sorted_asks[i]])
            else:
                values.append(0)
        return np.array(values)

class Booklist():
    """A class to store order books.

    Provides methods for writing to external databases.

    Example:
        Create a Booklist::

            >> booklist = hft.BookList(['GOOG', 'AAPL'], levels=10)

    Attributes:
        books (list): list of Books.

    """

    def __init__(self, names, levels):
        self.books = {}
        for name in names:
            self.books[name] = {'hist':[], 'cur':Book(levels)}

    def update(self, message):
        """Update order book data from message."""
        b = self.books[message.name]['cur'].update(message)
        self.books[message.name]['hist'].append(b)

    def to_hdf5(self, name, db):
        """Write order books to HDF5 file."""
        ob = self.books[name]['hist']
        listed = [book.to_array() for book in ob]
        array = np.array(listed)
        db_size, db_cols = db.orderbooks[name].shape  # rows
        array_size, array_cols = array.shape
        db_resize = db_size + array_size
        db.orderbooks[name].resize((db_resize,db_cols))
        db.orderbooks[name][db_size:db_resize,:] = array
        self.books[name]['cur'] = ob[-1]  # reset
        self.books[name]['hist'] = []  # reset
        # print('wrote {} lines to dataset {}'.format(array_size,
                                                    # db.orderbooks[name]))

    def to_postgres(self, date, name, db):
        """Write order books to PostgreSQL database."""
        # connect to postgres
        db.open()
        # get the message list for name
        ob = self.books[name]['hist']
        # convert the messages to a list of lists
        listed = [book.to_list(date=date, name=name) for book in ob]
        # write the messages/lists to the db
        with db.conn.cursor() as cursor:
            for book in listed:
                try:
                    cursor.execute('insert into orderbooks values%s;', [tuple(book)])  # %s becomes "(x,..., x)"
                except pg.Error as e:
                    print(e.pgerror)
        db.conn.commit()
        db.close()
        self.books[name]['cur'] = ob[-1]  # reset
        self.books[name]['hist'] = []  # reset
        print('wrote {} lines to orderbooks table for name {}'.format(len(ob),name))

def get_message_size(size_in_bytes):
    """Return number of bytes in binary message as an integer."""
    (message_size,) = struct.unpack('>H', size_in_bytes)
    return message_size

def get_message_type(type_in_bytes):
    """Return the type of a binary message as a string."""
    return type_in_bytes.decode('ascii')

def get_message(message_bytes, message_type, time, version):
    """Return binary message data as a Message."""
    if message_type in ('T', 'S', 'H', 'A', 'F', 'E', 'C', 'X', 'D', 'U', 'Q'):
        message = protocol(message_bytes, message_type, time, version)
        if version == 5.0:
            message.sec = int(message.nano / 10**9)
            message.nano = message.nano % 10**9
        return message
    else:
        return None

def protocol(message_bytes, message_type, time, version):
    """Decode binary message data and return as a Message."""
    message = Message()
    message.type = message_type

    if version == 4.0:
        if message.type == 'T':  # time
            temp = struct.unpack('>I', message_bytes)
            message.sec = temp[0]
            message.nano = 0
        elif message_type == 'S':  # systems
            temp = struct.unpack('>Is', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.event = temp[1].decode('ascii')
        elif message_type == 'H':  # trade-action
            temp = struct.unpack('>I6sss4s', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.name = temp[1].decode('ascii').rstrip(' ')
            message.event = temp[2].decode('ascii')
        elif message.type == 'A':  # add
            temp = struct.unpack('>IQsI6sI', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.refno = temp[1]
            message.buysell = temp[2].decode('ascii')
            message.shares = temp[3]
            message.name = temp[4].decode('ascii').rstrip(' ')
            message.price = temp[5]
        elif message.type == 'F':  # add w/mpid (I ignore mpid, so same as 'A')
            temp = struct.unpack('>IQsI6sI4s', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.refno = temp[1]
            message.buysell = temp[2].decode('ascii')
            message.shares = temp[3]
            message.name = temp[4].decode('ascii').rstrip(' ')
            message.price = temp[5]
        elif message.type == 'E':  # execute
            temp = struct.unpack('>IQIQ', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.refno = temp[1]
            message.shares = temp[2]
        elif message.type == 'C':  # execute w/price (actually don't need price...)
            temp = struct.unpack('>IQIQsI', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.refno = temp[1]
            message.shares = temp[2]
            message.price = temp[5]
        elif message.type == 'X':  # cancel
            temp = struct.unpack('>IQI', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.refno = temp[1]
            message.shares = temp[2]
        elif message.type == 'D':  # delete
            temp = struct.unpack('>IQ', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.refno = temp[1]
        elif message.type == 'U':  # replace
            temp = struct.unpack('>IQQII', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.refno = temp[1]
            message.newrefno = temp[2]
            message.shares = temp[3]
            message.price = temp[4]
        elif message.type == 'Q':
            temp = struct.unpack('>IQ6sIQs', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.shares = temp[1]
            message.name = temp[2].decode('ascii').rstrip(' ')
            message.price = temp[3]
            message.event = temp[5].decode('ascii')
        return message
    elif version == 4.1:
        if message.type == 'T':  # time
            temp = struct.unpack('>I', message_bytes)
            message.sec = temp[0]
            message.nano = 0
        elif message_type == 'S':  # systems
            temp = struct.unpack('>Is', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.event = temp[1].decode('ascii')
        elif message.type == 'H':  # trade-action
            temp = struct.unpack('>I8sss4s', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.name = temp[1].decode('ascii').rstrip(' ')
            message.event = temp[2].decode('ascii')
        elif message.type == 'A':  # add
            temp = struct.unpack('>IQsI8sI', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.refno = temp[1]
            message.buysell = temp[2].decode('ascii')
            message.shares = temp[3]
            message.name = temp[4].decode('ascii').rstrip(' ')
            message.price = temp[5]
        elif message.type == 'F':  # add w/mpid
            temp = struct.unpack('>IQsI8sI4s', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.refno = temp[1]
            message.buysell = temp[2].decode('ascii')
            message.shares = temp[3]
            message.name = temp[4].decode('ascii').rstrip(' ')
            message.price = temp[5]
        elif message.type == 'E':  # execute
            temp = struct.unpack('>IQIQ', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.refno = temp[1]
            message.shares = temp[2]
        elif message.type == 'C':  # execute w/price
            temp = struct.unpack('>IQIQsI', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.refno = temp[1]
            message.shares = temp[2]
            message.price = temp[5]
        elif message.type == 'X':  # cancel
            temp = struct.unpack('>IQI', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.refno = temp[1]
            message.shares = temp[2]
        elif message.type == 'D':  # delete
            temp = struct.unpack('>IQ', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.refno = temp[1]
        elif message.type == 'U':  # replace
            temp = struct.unpack('>IQQII', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.refno = temp[1]
            message.newrefno = temp[2]
            message.shares = temp[3]
            message.price = temp[4]
        elif message.type == 'Q':  # cross-trade
            temp = struct.unpack('>IQ8sIQs', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.shares = temp[1]
            message.name = temp[2].decode('ascii').rstrip(' ')
            message.price = temp[3]
            message.event = temp[5].decode('ascii')
        return message
    elif version == 5.0:
        if message.type == 'T':  # time
            raise ValueError('Time messages not supported in ITCHv5.0.')
        elif message_type == 'S':  # systems
            temp = struct.unpack('>HHHIs', message_bytes)
            message.sec = time
            message.nano = temp[2] | (temp[3] << 16)
            message.event = temp[4].decode('ascii')
        elif message.type == 'H':
            temp = struct.unpack('>HHHI8sss4s', message_bytes)
            message.sec = time
            message.nano = temp[2] | (temp[3] << 16)
            message.name = temp[4].decode('ascii').rstrip(' ')
            message.event = temp[5].decode('ascii')
        elif message.type == 'A':  # add
            temp = struct.unpack('>HHHIQsI8sI', message_bytes)
            message.sec = time
            message.nano = temp[2] | (temp[3] << 16)
            message.refno = temp[4]
            message.buysell = temp[5].decode('ascii')
            message.shares = temp[6]
            message.name = temp[7].decode('ascii').rstrip(' ')
            message.price = temp[8]
        elif message.type == 'F':  # add w/mpid
            temp = struct.unpack('>HHHIQsI8sI', message_bytes)
            message.sec = time
            message.nano = temp[2] | (temp[3] << 16)
            message.refno = temp[4]
            message.buysell = temp[5].decode('ascii')
            message.shares = temp[6]
            message.name = temp[7].decode('ascii').rstrip(' ')
            message.price = temp[8]
        elif message.type == 'E':  # execute
            temp = struct.unpack('>HHHIQIQ', message_bytes)
            message.sec = time
            message.nano = temp[2] | (temp[3] << 16)
            message.refno = temp[4]
            message.shares = temp[5]
        elif message.type == 'C':  # execute w/price
            temp = struct.unpack('>HHHIQIQsI', message_bytes)
            message.sec = time
            message.nano = temp[2] | (temp[3] << 16)
            message.refno = temp[4]
            message.shares = temp[5]
            message.price = temp[8]
        elif message.type == 'X':  # cancel
            temp = struct.unpack('>HHHIQI', message_bytes)
            message.sec = time
            message.nano = temp[2] | (temp[3] << 16)
            message.refno = temp[4]
            message.shares = temp[5]
        elif message.type == 'D':  # delete
            temp = struct.unpack('>HHHIQ', message_bytes)
            message.sec = time
            message.nano = temp[2] | (temp[3] << 16)
            message.refno = temp[4]
        elif message.type == 'U':  # replace
            temp = struct.unpack('>HHHIQQII', message_bytes)
            message.sec = time
            message.nano = temp[2] | (temp[3] << 16)
            message.refno = temp[4]
            message.newrefno = temp[5]
            message.shares = temp[6]
            message.price = temp[7]
        elif message.type == 'Q':  # cross-trade
            temp = struct.unpack('>HHHI', message_bytes)
            message.sec = time
            message.nano = temp[2] | (temp[3] << 16)
            message.shares = temp[4]
            message.name = temp[5].decode('ascii').rstrip(' ')
            message.price = temp[6]
            message.event = temp[8].decode('ascii')
        return message
    else:
        raise ValueError('ITCH version ' + str(version) + ' is not supported')

def unpack(fin, ver, date, fout, nlevels, names, method=None):
    """Read ITCH data file, construct LOB, and write to database.

    This method reads binary data from ITCH dat file, converts it into human-readable data, then saves time series of out-going messages as well as reconstructed order book snapshots to research databases.

    The version number of the ITCH data must be specified as a float. Supported versions are: 4.0, 4.1, and 5.0.

    """

    MAXROWS = 10**4
    orderlist = Orderlist()
    booklist = Booklist(names, nlevels)
    messagelist = Messagelist(names)

    if method == 'hdf5':
        db = Database(fout, names, nlevels)
    elif method == 'postgres':
        db = Postgres(host='localhost', user='colinswaney', nlevels=nlevels)
    elif method == 'csv':
        pass
    else:
        print('No database option specified. Creating csv files.')
    data = open(fin, 'rb')

    messagecount = 0

    reading = True
    writing = False
    clock = 0

    start = time.time()

    # unpacking
    while reading:

        # read message
        message_size = get_message_size(data.read(2))
        message_type = get_message_type(data.read(1))
        message_bytes = data.read(message_size - 1)
        message = get_message(message_bytes, message_type, clock, ver)
        messagecount += 1

        # update clock
        if message_type == 'T':
            if message.sec % 60 == 0:
                # print('TIME={}'.format(message.sec))
                pass
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
            # input('PAUSED (press any button to continue).')

        if message_type == 'H':
            # print('TRADE-ACTION MESSAGE: {}'.format(message.event))
            if message.event in ('H','P','V'):
                pass  # remove message.name from names
            elif message.event == 'T':
                pass  # add message.name to names (check that it isn't there already)
            elif message.event in ('Q','R'):
                pass  # quote only (only accepting A, F, X, D, U)

        # complete message
        if message_type == 'U':
            del_message, add_message = message.split()
            orderlist.complete_message(del_message)
            orderlist.complete_message(add_message)
        elif message_type in ('E', 'C', 'X', 'D'):
            orderlist.complete_message(message)

        # update orders
        if message_type == 'U':
            if del_message.name in names:
                orderlist.update(del_message)
                # print(del_message)
            if add_message.name in names:
                orderlist.add(add_message)
                # print(add_message)
        elif message_type in ('E', 'C', 'X', 'D'):
            if message.name in names:
                orderlist.update(message)
                # print(message)
        elif message_type in ('A', 'F'):
            if message.name in names:
                orderlist.add(message)
                # print(message)

        # update messages, books, and write to disk
        if method == 'hdf5':
            if message_type == 'U':
                if del_message.name in names:
                    # update messages
                    messagelist.add(del_message)
                    # print('{} message added to list'.format(del_message.type))
                    msgs = len(messagelist.messages[del_message.name])
                    if msgs == MAXROWS:
                        messagelist.to_hdf5(name=del_message.name, db=db)
                        # input('Press any button to continue.')
                    # update books
                    booklist.update(del_message)
                    # print('{} book was updated.'.format(del_message.name))
                    bks = len(booklist.books[del_message.name]['hist'])
                    if bks == MAXROWS:
                        booklist.to_hdf5(name=del_message.name, db=db)
                        # input('Press any button to continue.')
                if add_message.name in names:
                    # update messages
                    messagelist.add(add_message)
                    # print('{} message added to list'.format(add_message.type))
                    msgs = len(messagelist.messages[add_message.name])
                    if msgs == MAXROWS:
                        messagelist.to_hdf5(name=add_message.name, db=db)
                        # input('Press any button to continue.')
                    # update books
                    booklist.update(add_message)
                    # print('{} book was updated.'.format(add_message.name))
                    bks = bks = len(booklist.books[add_message.name]['hist'])
                    if bks == MAXROWS:
                        booklist.to_hdf5(name=add_message.name, db=db)
                        # input('Press any button to continue.')
            elif message_type in ('A', 'F', 'E', 'C', 'X', 'D'):
                if message.name in names:
                    # update messages
                    messagelist.add(message)
                    # print('{} message added to list'.format(message.type))
                    msgs = len(messagelist.messages[message.name])
                    if msgs == MAXROWS:
                        messagelist.to_hdf5(name=message.name, db=db)
                        # input('Press any button to continue.')
                    # update books
                    booklist.update(message)
                    # print('{} book was updated.'.format(message.name))
                    bks = len(booklist.books[message.name]['hist'])
                    if bks == MAXROWS:
                        booklist.to_hdf5(name=message.name, db=db)
                        # input('Press any button to continue.')
        elif method == 'postgres':
            if message_type == 'U':
                if del_message.name in namelist:
                    # update messages
                    messagelist.add(del_message)
                    # print('{} message added to list'.format(del_message.type))
                    msgs = len(messagelist.messages[del_message.name])
                    if msgs == MAXROWS:
                        messagelist.to_postgres(date=date,
                                                name=del_message.name,
                                                db=db)
                    # update books
                    booklist.update(del_message)
                    # print('{} book was updated.'.format(del_message.name))
                    bks = len(booklist.books[del_message.name]['hist'])
                    if bks == MAXROWS:
                        booklist.to_postgres(date=date,
                                             name=del_message.name,
                                             db=db)
                if add_message.name in namelist:
                    # update messages
                    messagelist.add(add_message)
                    # print('{} message added to list'.format(add_message.type))
                    msgs = len(messagelist.messages[add_message.name])
                    if msgs == MAXROWS:
                        messagelist.to_postgres(date=date,
                                                name=add_message.name,
                                                db=db)
                    # update books
                    booklist.update(add_message)
                    # print('{} book was updated.'.format(add_message.name))
                    bks = len(booklist.books[add_message.name]['hist'])
                    if bks == MAXROWS:
                        booklist.to_postgres(date=date,
                                             name=add_message.name,
                                             db=db)
            elif message_type in ('A', 'F', 'E', 'C', 'X', 'D'):
                if message.name in namelist:
                    # update messages
                    messagelist.add(message)
                    # print('{} message added to list'.format(message.type))
                    msgs = len(messagelist.messages[message.name])
                    if msgs == MAXROWS:
                        messagelist.to_postgres(date=date,
                                                name=message.name,
                                                db=db)
                    # update books
                    booklist.update(message)
                    # print('{} book was updated.'.format(message.name))
                    bks = len(booklist.books[message.name]['hist'])
                    if bks == MAXROWS:
                        booklist.to_postgres(date=date,
                                             name=message.name,
                                             db=db)
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

def load(db, name):
    """Read data from database and return pd.DataFrames."""
    try:
        with h5py.File(db, 'r') as f: # read, file must exist
            try:  # get message data
                messages = f['/messages/' + name]
                data = messages[:,:]
                T,N = data.shape
                f.close()
                columns = ['sec',
                           'nano',
                           'type',
                           'event',
                           'buysell',
                           'price',
                           'shares',
                           'refno',
                           'newrefno']
                df_message = pd.DataFrame(data,
                                          index=np.arange(0,T),
                                          columns=columns)
            except KeyError as e:
                print('Could not find name {} in messages'.format(name))
            try:  # get orderbook data
                data = f['orderbooks/' + name]
                nlevels = int((data.shape[1] - 2) / 4)
                pidx = list(range(2, 2 + nlevels))
                pidx.extend(list(range(2 + nlevels, 2 + 2*nlevels)))
                vidx = list(range(2 + 2*nlevels, 2 + 3*nlevels))
                vidx.extend(list(range(2 + 3*nlevels, 2 + 4*nlevels)))
                timestamps = data[:,0:2]
                prices = data[:, pidx]
                volume = data[:, vidx]
                base_columns = [str(i) for i in list(range(1, nlevels + 1))]
                price_columns = ['bidprc.' + i for i in base_columns]
                volume_columns = ['bidvol.' + i for i in base_columns]
                price_columns.extend(['askprc.' + i for i in base_columns])
                volume_columns.extend(['askvol.' + i for i in base_columns])
                df_time = pd.DataFrame(timestamps, columns=['sec','nano'])
                df_price = pd.DataFrame(prices, columns=price_columns)
                df_volume = pd.DataFrame(volume, columns=volume_columns)
                df_price = pd.concat([df_time, df_price], axis=1)
                df_volume = pd.concat([df_time, df_volume], axis=1)
            except KeyError as e:
                print('Could not find name {} in orderbooks'.format(name))
            return (df_message, df_price, df_volume)
    except OSError as e:
        print('Could not find file {}'.format(path))

def interpolate(data, tstep):
    """Interpolate limit order data.

    Uses left-hand interpolation, and assumes that the data is indexed by timestamp.

    """
    T,N = data.shape
    timestamps = data.index
    t0 = timestamps[0] - (timestamps[0] % tstep)  # 34200
    tN = timestamps[-1] - (timestamps[-1] % tstep) + tstep  # 57600
    timestamps_new = np.arange(t0 + tstep, tN + tstep, tstep)  # [34200, ..., 57600]
    X = np.zeros((len(timestamps_new),N))  # np.array
    X[-1,:] = data.values[-1,:]
    t = timestamps_new[0]  # keeps track of time in NEW sampling frequency
    for i in np.arange(0,T):  # observations in data...
        if timestamps[i] > t:
            s = timestamps[i] - (timestamps[i] % tstep)
            tidx = int((t - t0) / tstep - 1)
            sidx = int((s - t0) / tstep)  # plus one for python indexing (below)
            X[tidx:sidx,:] = data.values[i-1,:]
            t = s + tstep
        else:
            pass
    return pd.DataFrame(X,
                        index=timestamps_new,
                        columns=data.columns)

def imshow(data):
    """Display order book data as an image."""
    levels = int(data.shape[1] / 2)
    idx = ['askvol.' + str(i) for i in range(levels, 0, -1)]
    idx.extend(['bidvol.' + str(i) for i in range(1, levels + 1, 1)])
    plt.imshow(data.ix[:,idx].T, interpolation='nearest', aspect='auto', cmap='gray')

def reorder(data, columns):
    """Reorder the columns of order data.

    The resulting columns will be asks (high-to-low) followed by bids (low-to-high).

    """
    levels = int((data.shape[1] - 2) / 2)
    if columns == 'volume' or type == 'v':
        idx = ['askvol.' + str(i) for i in range(levels, 0, -1)]
        idx.extend(['bidvol.' + str(i) for i in range(1, levels + 1, 1)])
    elif columns == 'price' or type == 'p':
        idx = ['askprc.' + str(i) for i in range(levels, 0, -1)]
        idx.extend(['bidprc.' + str(i) for i in range(1, levels + 1, 1)])
    return data.ix[:,idx]
