import numpy as np
import pandas as pd
import psycopg2 as pg
import matplotlib.pyplot as plt
import struct
import h5py
import time

class Database():
    """Connection to an HDF5 database storing message and order book data.

    Parameters
    ----------
    path : string
        Specifies location of the HDF5 file
    names : list
        Contains the stock tickers to include in the database
    nlevels : int
        Specifies the number of levels to include in the order book data

    """

    def __init__(self, path, names, nlevels):
        # open file, create otherwise
        try:
            self.file = h5py.File(path, 'r+')  # read/write, file must exist
            print('Appending existing HDF5 file.')
            for name in names:
                if name in self.file['messages'].keys():
                    print('Overwriting message data for {}'.format(name))
                    del self.file['messages'][name]
                if name in self.file['orderbooks'].keys():
                    print('Overwriting orderbook data for {}'.format(name))
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
    """A connection to a PostgreSQL database to storing message and order book data.

    Parameters
    ----------
    host : string
        Host for the Postgres connection
    user : string
        Username for the Postgres connection
    nlevels : int
        Specifies the number of levels to include in the order book data

    """

    def __init__(self, host, user, nlevels):

        self.host = host
        self.user = user
        self.nlevels = nlevels

        # create column info
        msg_sql = """create table messages (date varchar,
                                            name varchar,
                                            sec integer,
                                            nano integer,
                                            type varchar,
                                            event varchar,
                                            buysell varchar,
                                            price integer,
                                            shares integer,
                                            refno integer,
                                            newrefno integer)"""

        cols = ['date date',
                'name varchar',
                'sec integer',
                'nano integer']
        cols.extend(['bid_prc_' + str(i) + ' integer' for i in range(1,nlevels+1)])
        cols.extend(['ask_prc_' + str(i) + ' integer' for i in range(1,nlevels+1)])
        cols.extend(['bid_vol_' + str(i) + ' integer' for i in range(1,nlevels+1)])
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
            print('Created a new PostgreSQL database.')
        except pg.Error as e:
            print('ERROR: unable to connect to database.')

    def open(self):
        self.conn = pg.connect(host=self.host, user=self.user)

    def close(self):
        self.conn.close()

class Message():
    """A class representing out-going messages from the NASDAQ system.

    Parameters
    ----------
    sec : int
        Seconds
    nano : int
        Nanoseconds
    type : string
        Message type
    event : string
        System event
    name : string
        Stock ticker
    buysell : string
        Trade position
    price : int
        Trade price
    shares : int
        Shares
    refno : int
        Unique reference number of order
    newrefno : int
        Replacement reference number
    mpid: string
        MPID attribution
    """

    def __init__(self, date='.', sec=-1, nano=-1, type='.', event='.', name='.',
                 buysell='.', price=-1, shares=0, refno=-1, newrefno=-1, mpid='.'):
        self.date = date
        self.name = name
        self.sec = sec
        self.nano = nano
        self.type = type
        self.event = event
        self.buysell = buysell
        self.price = price
        self.shares = shares
        self.refno = refno
        self.newrefno = newrefno
        self.mpid = mpid

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
                'newrefno=' + str(self.newrefno),
                'mpid= {}'.format(self.mpid)]
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
                'newrefno: ' + str(self.newrefno),
                'mpid: {}'.format(self.mpid)]
        return 'Message(' + sep.join(line) + ')'

    def split(self):
        """Converts a replace message to an add and a delete."""
        if self.type == 'U':
            del_message = Message(date=self.date,
                                  sec=self.sec,
                                  nano=self.nano,
                                  type='D',
                                  refno=self.refno,
                                  newrefno=-1)
            add_message = Message(date=self.date,
                                  sec=self.sec,
                                  nano=self.nano,
                                  type='U',
                                  price=self.price,
                                  shares=self.shares,
                                  refno=self.refno,
                                  newrefno=self.newrefno)
            return (del_message, add_message)
        else:
            print('Warning: "split" method called on non-replacement messages.')

    def to_list(self):
        """Returns message as a list."""

        values = []
        values.append(str(self.date))
        values.append(str(self.name))
        values.append(int(self.sec))
        values.append(int(self.nano))
        values.append(str(self.type))
        values.append(str(self.event))
        values.append(str(self.buysell))
        values.append(int(self.price))
        values.append(int(self.shares))
        values.append(int(self.refno))
        values.append(int(self.newrefno))
        values.append(int(self.mpid))
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
        elif self.type == 'P':  # trade (hidden)
            values.append(8)
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

    def to_txt(self, fout):
        if self.type == 'S':
            sep = ','
            line = [str(self.sec),
                    str(self.nano),
                    str(self.event)]
            fout.write(sep.join(line) + '\n')
        elif self.type == 'H':
            sep = ','
            line = [str(self.sec),
                    str(self.nano),
                    str(self.name),
                    str(self.event)]
            fout.write(sep.join(line) + '\n')
        elif self.type in ('A', 'F', 'E', 'C', 'X', 'D'):
            sep = ','
            line = [str(self.sec),
                    str(self.nano),
                    str(self.name),
                    str(self.type),
                    str(self.refno),
                    str(self.buysell),
                    str(self.shares),
                    str(self.price / 10 ** 4),
                    str(self.mpid)]
            fout.write(sep.join(line) + '\n')
        elif self.type == 'P':
            sep = ','
            line = [str(self.sec),
                    str(self.nano),
                    str(self.name),
                    str(self.buysell),
                    str(self.shares),
                    str(self.price / 10 ** 4)]
            fout.write(sep.join(line) + '\n')

class Messagelist():
    """A class to store messages.

    Provides methods for writing to HDF5 and PostgreSQL databases.

    Parameters
    ----------
    date : string
        Date to be assigned to data
    names : list
        Contains the stock tickers to include in the database

    Attributes
    ----------
    messages : dict
        Contains a Message objects for each name in names

    Examples
    --------
    Create a MessageList::

    >> msglist = hft.Messagelist(date='01012013', names=['GOOG', 'AAPL'])

    """

    def __init__(self, date, names):
        self.messages = {}
        self.date = date
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
        if len(m) > 0:
            listed = [message.to_array() for message in m]
            array = np.array(listed)
            db_size, db_cols = db.messages[name].shape  # rows
            array_size, array_cols = array.shape
            db_resize = db_size + array_size
            db.messages[name].resize((db_resize,db_cols))
            db.messages[name][db_size:db_resize,:] = array
            self.messages[name] = []  # reset
        print('wrote {} messages to dataset (name={})'.format(len(m), name))

    def to_postgres(self, name, db):
        """Write messages to PostgreSQL database."""
        db.open()
        m = self.messages[name]
        listed = [message.to_list() for message in m]
        with db.conn.cursor() as cursor:
            for message in listed:
                try:
                    cursor.execute('insert into messages values%s;',
                                   [tuple(message)])  # %s becomes "(x,..., x)"
                except pg.Error as e:
                    print(e.pgerror)
        db.conn.commit()
        db.close()
        self.messages[name] = [] # reset
        print('wrote {} messages to table (name={})'.format(len(m), name))

class NOIIMessage():
    """A class representing out-going messages from the NASDAQ system.
       This class is specific to net order imbalance indicator messages and
       cross trade messages.

    Parameters
    ----------
    sec: int
        Seconds
    nano: int
        Nanoseconds
    name: string
        Stock ticker
    type: string
        Message type
    cross: string
        Cross type
    buysell: string
        Trade position
    price: int
        Trade price
    shares: int
        Shares
    matchno: int
        Unique reference number of trade
    paired: int
        Shares paired
    imbalance: int
        Shares imbalance
    direction: string
        Imbalance direction
    far: int
        Far price
    near: int
        Near price
    current: int
        Current refernce price

    """

    def __init__(self, date='.', sec=-1, nano=-1, name='.', type='.', cross='.',
                 buysell='.', price=-1, shares=0, matchno=-1, paired=-1,
                 imbalance=-1, direction='.', far=-1, near=-1, current=-1):
        self.date = date
        self.sec = sec
        self.nano = nano
        self.name = name
        self.type = type
        self.cross = cross
        self.buysell = buysell
        self.price = price
        self.shares = shares
        self.matchno = matchno
        self.paired = paired
        self.imbalance = imbalance
        self.direction = direction
        self.far = far
        self.near = near
        self.current = current

    def __str__(self):
        sep = ', '
        line = ['date=' + str(self.date),
                'sec=' + str(self.sec),
                'nano=' + str(self.nano),
                'name=' + str(self.name),
                'type=' + str(self.type),
                'cross=' + str(self.cross),
                'buysell=' + str(self.buysell),
                'price=' + str(self.price),
                'shares=' + str(self.shares),
                'matchno=' + str(self.matchno),
                'paired=' + str(self.paired),
                'imbalance=' + str(self.imbalance),
                'direction=' + str(self.direction),
                'far=' + str(self.far),
                'near=' + str(self.near),
                'current=' + str(self.current)]
        return sep.join(line)

    def __repr__(self):
        sep = ', '
        line = ['date=' + str(self.date),
                'sec=' + str(self.sec),
                'nano=' + str(self.nano),
                'name=' + str(self.name),
                'type=' + str(self.type),
                'cross=' + str(self.cross),
                'buysell=' + str(self.buysell),
                'price=' + str(self.price),
                'shares=' + str(self.shares),
                'matchno=' + str(self.matchno),
                'paired=' + str(self.paired),
                'imbalance=' + str(self.imbalance),
                'direction=' + str(self.direction),
                'far=' + str(self.far),
                'near=' + str(self.near),
                'current=' + str(self.current)]
        return 'Message(' + sep.join(line) + ')'

    def to_list(self):
        """Returns message as a list."""

        values = []
        values.append(str(self.date))
        values.append(int(self.sec))
        values.append(int(self.nano))
        values.append(str(self.name))
        values.append(str(self.type))
        values.append(str(self.cross))
        values.append(str(self.buysell))
        values.append(int(self.price))
        values.append(int(self.shares))
        values.append(int(self.matchno))
        values.append(int(self.paired))
        values.append(int(self.imbalance))
        values.append(int(self.direction))
        values.append(int(self.far))
        values.append(int(self.near))
        values.append(int(self.current))
        return values

    def to_array(self):
        """Returns message as an np.array of integers."""

        values = []
        values.append(int(self.sec))
        values.append(int(self.nano))

        if self.type == 'Q':  # cross trade
            values.append(0)
        elif self.type == 'I':  # noii
            values.append(1)
        else:
            values.append(-1)  # other (ignored)
            print('Unexpected NOII message type: {}'.format(self.type))

        if self.cross == 'O':    # opening cross
            values.append(0)
        elif self.cross == 'C':  # closing cross
            values.append(1)
        elif self.cross == 'H':  # halted cross
            values.append(2)
        elif self.cross == 'I':  # intraday cross
            values.append(3)
        else:
            print('Unexpected cross type: {}'.format(self.cross))
            values.append(-1)  # mistake occurred

        if self.buysell == 'B':  # bid
            values.append(1)
        elif self.buysell == 'S':  # ask
            values.append(-1)
        else:
            values.append(0)

        values.append(int(self.price))
        values.append(int(self.shares))
        values.append(int(self.matchno))
        values.append(int(self.paired))
        values.append(int(self.imbalance))

        if self.direction == 'B':  # bid
            values.append(1)
        elif self.direction == 'S':  # ask
            values.append(-1)
        else:
            values.append(0)

        values.append(int(self.far))
        values.append(int(self.near))
        values.append(int(self.current))

        return np.array(values)

    def to_txt(self, fout):
        sep = ','
        if self.type == 'Q':
            line = [str(self.sec),
                    str(self.nano),
                    str(self.name),
                    str(self.type),
                    str(self.cross),
                    str(self.shares),
                    str(self.price / 10 ** 4),
                    str(self.paired),
                    str(self.imbalance),
                    str(self.direction),
                    str(self.far),
                    str(self.near),
                    str(self.current)]
            fout.write(sep.join(line) + '\n')
        elif self.type == 'I':
            line = [str(self.sec),
                    str(self.nano),
                    str(self.name),
                    str(self.type),
                    str(self.cross),
                    str(self.shares),
                    str(self.price),
                    str(self.paired),
                    str(self.imbalance),
                    str(self.direction),
                    str(self.far / 10 ** 4),
                    str(self.near / 10 ** 4),
                    str(self.current / 10 ** 4)]
            fout.write(sep.join(line) + '\n')

# class TradingActionMessage():
#     """
#
#     Parameters
#     ----------
#     date: string
#     sec: int
#     nano: int
#     name: string
#     state: string
#
#     """
#
#     def __init__(self, date='.', sec=-1, nano=-1, name='.', state='.'):
#         self.date = date
#         self.sec = sec
#         self.nano = nano
#         self.name = name
#         self.state = state
#
#     def __str__(self):
#         sep = ', '
#         line = ['date=' + str(self.date),
#                 'sec=' + str(self.sec),
#                 'nano=' + str(self.nano),
#                 'name=' + str(self.name),
#                 'state=' + str(self.state)]
#         return sep.join(line)
#
#     def __repr__(self):
#         sep = ', '
#         line = ['date=' + str(self.date),
#                 'sec=' + str(self.sec),
#                 'nano=' + str(self.nano),
#                 'name=' + str(self.name),
#                 'state=' + str(self.state)]
#         return 'Message(' + sep.join(line) + ')'
#
#     def to_list(self):
#         """Returns message as a list."""
#
#         values = []
#         values.append(str(self.date))
#         values.append(int(self.sec))
#         values.append(int(self.nano))
#         values.append(str(self.name))
#         values.append(str(self.state))
#         return values
#
#     def to_txt(self, fout):
#         line = [str(self.sec),
#                 str(self.nano),
#                 str(self.name),
#                 str(self.state)]
#         fout.write(','.join(line) + '\n')

class Order():
    """A class to represent limit orders.

    Stores essential message data for order book reconstruction.

    Attributes
    ----------
    name : string
        Stock ticker
    buysell : string
        Trade position
    price : int
        Trade price
    shares : int
        Shares

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

    Attributes
    ----------
    orders : dict
        Keys are reference numbers, values are Orders.

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
        """Look up Order for Message and fill in missing data."""
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
        """Add a new Order to the list."""
        order = Order()
        order.name = message.name
        order.buysell = message.buysell
        order.price = message.price
        order.shares = message.shares
        self.orders[message.refno] = order

    def update(self, message):
        """Update an existing Order based on incoming Message."""
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

    This class provides a method for updating the state of an order book from an
    incoming message.

    Attributes
    ----------
    bids : dict
        Keys are prices, values are shares
    asks : dict
        Keys are prices, values are shares
    levels : int
        Number of levels of the the order book to track
    sec : int
        Seconds
    nano : int
        Nanoseconds

    """

    def __init__(self, date, name, levels):
        self.bids = {}
        self.asks = {}
        self.min_bid = -np.inf
        self.max_ask = np.inf
        self.levels = levels
        self.sec = -1
        self.nano = -1
        self.date = date
        self.name = name

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
        """Update Order using incoming Message data."""
        self.sec = message.sec
        self.nano = message.nano
        updated = False
        if message.buysell == 'B':
            if message.price in self.bids.keys():
                self.bids[message.price] += message.shares
                if self.bids[message.price] == 0:
                    self.bids.pop(message.price)
            elif message.type in ('A','F'):
                self.bids[message.price] = message.shares
        elif message.buysell == 'S':
            if message.price in self.asks.keys():
                self.asks[message.price] += message.shares
                if self.asks[message.price] == 0:
                    self.asks.pop(message.price)
            elif message.type in ('A','F'):
                self.asks[message.price] = message.shares
        return self

    def to_list(self):
        """Return Order as a list."""
        values = []
        values.append(self.date)
        values.append(self.name)
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
        return values

    def to_array(self):
        '''Return Order as numpy array.'''
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

    def to_txt(self, fout):
        values = []
        values.append(int(self.sec))
        values.append(int(self.nano))
        values.append(self.name)
        sorted_bids = sorted(self.bids.keys(), reverse=True)
        sorted_asks = sorted(self.asks.keys())
        for i in range(0, self.levels): # bid price
            if i < len(self.bids):
                values.append(sorted_bids[i] / 10 ** 4)
            else:
                values.append(-1)
        for i in range(0, self.levels): # ask price
            if i < len(self.asks):
                values.append(sorted_asks[i] / 10 ** 4)
            else:
                values.append(-1)
        for i in range(0, self.levels): # bid depth
            if i < len(self.bids):
                values.append(self.bids[sorted_bids[i]])
            else:
                values.append(-1)
        for i in range(0, self.levels): # ask depth
            if i < len(self.asks):
                values.append(self.asks[sorted_asks[i]])
            else:
                values.append(-1)
        fout.write(','.join([str(v) for v in values]) + '\n')

class Booklist():
    """A class to store Books.

    Provides methods for writing to external databases.

    Examples
    --------
    Create a Booklist::

    >> booklist = hft.BookList(['GOOG', 'AAPL'], levels=10)

    Attributes
    ----------
    books : list
        A list of Books
    method : string
        Specifies the type of database to create ('hdf5' or 'postgres')

    """

    def __init__(self, date, names, levels, method):
        self.books = {}
        self.method = method
        for name in names:
            self.books[name] = {'hist':[], 'cur':Book(date, name, levels)}

    def update(self, message):
        """Update Book data from message."""
        b = self.books[message.name]['cur'].update(message)
        if self.method == 'hdf5':
            self.books[message.name]['hist'].append(b.to_array())
        if self.method == 'postgres':
            self.books[message.name]['hist'].append(b.to_list())

    def to_hdf5(self, name, db):
        """Write Book data to HDF5 file."""
        hist = self.books[name]['hist']
        if len(hist) > 0:
            array = np.array(hist)
            db_size, db_cols = db.orderbooks[name].shape  # rows
            array_size, array_cols = array.shape
            db_resize = db_size + array_size
            db.orderbooks[name].resize((db_resize,db_cols))
            db.orderbooks[name][db_size:db_resize,:] = array
            self.books[name]['hist'] = []  # reset
        print('wrote {} books to dataset (name={})'.format(len(hist), name))

    def to_postgres(self, name, db):
        """Write Book data to PostgreSQL database."""
        db.open()
        hist = self.books[name]['hist']
        with db.conn.cursor() as cursor:
            for book in hist:
                try:
                    cursor.execute('insert into orderbooks values%s;', [tuple(book)])  # %s becomes "(x,..., x)"
                except pg.Error as e:
                    print(e.pgerror)
        db.conn.commit()
        db.close()
        self.books[name]['hist'] = []  # reset
        print('wrote {} books to table (name={})'.format(len(hist),name))


# class Book():
#     """A class to represent an order book.
#
#     This class provides a method for updating the state of an order book from an
#     incoming message.
#
#     Attributes
#     ----------
#     bids : dict
#         Keys are prices, values are shares
#     asks : dict
#         Keys are prices, values are shares
#     levels : int
#         Number of levels of the the order book to track
#     sec : int
#         Seconds
#     nano : int
#         Nanoseconds
#
#     """
#
#     def __init__(self, date, name, levels):
#         self.bids = {}
#         self.asks = {}
#         self.levels = levels
#         self.sec = -1
#         self.nano = -1
#         self.date = date
#         self.name = name
#
#     def __str__(self):
#         sep = ', '
#         sorted_bids = sorted(self.bids.keys(), reverse=True)  # high-to-low
#         sorted_asks = sorted(self.asks.keys())  # low-to-high
#         bid_list = []
#         ask_list = []
#         nbids = len(self.bids)
#         nasks = len(self.asks)
#         for i in range(0, self.levels):
#             if i < nbids:
#                 bid_list.append(str(self.bids[sorted_bids[i]]) + '@' + str(sorted_bids[i]))
#             else:
#                 pass
#             if i < nasks:
#                 ask_list.append(str(self.asks[sorted_asks[i]]) + '@' + str(sorted_asks[i]))
#             else:
#                 pass
#         return 'bids: ' + sep.join(bid_list) + '\n' + 'asks: ' + sep.join(ask_list)
#
#     def __repr__(self):
#         sep = ', '
#         sorted_bids = sorted(self.bids.keys(), reverse=True)  # high-to-low
#         sorted_asks = sorted(self.asks.keys())  # low-to-high
#         bid_list = []
#         ask_list = []
#         nbids = len(self.bids)
#         nasks = len(self.asks)
#         for i in range(0, self.levels):
#             if i < nbids:
#                 bid_list.append(str(self.bids[sorted_bids[i]]) + '@' + str(sorted_bids[i]))
#             else:
#                 pass
#             if i < nasks:
#                 ask_list.append(str(self.asks[sorted_asks[i]]) + '@' + str(sorted_asks[i]))
#             else:
#                 pass
#         return 'Book( \n' + 'bids: ' + sep.join(bid_list) + '\n' + 'asks: ' + sep.join(ask_list) + ' )'
#
#     def update(self, message):
#         """Update Order using incoming Message data."""
#         self.sec = message.sec
#         self.nano = message.nano
#         if message.buysell == 'B':
#             if message.price in self.bids.keys():
#                 self.bids[message.price] += message.shares
#                 if self.bids[message.price] == 0:
#                     self.bids.pop(message.price)
#             else:
#                 if message.type in ('A','F'):
#                     self.bids[message.price] = message.shares
#         elif message.buysell == 'S':
#             if message.price in self.asks.keys():
#                 self.asks[message.price] += message.shares
#                 if self.asks[message.price] == 0:
#                     self.asks.pop(message.price)
#             else:
#                 if message.type in ('A','F'):
#                     self.asks[message.price] = message.shares
#         return self
#
#     def to_list(self):
#         """Return Order as a list."""
#         values = []
#         values.append(self.date)
#         values.append(self.name)
#         values.append(int(self.sec))
#         values.append(int(self.nano))
#         sorted_bids = sorted(self.bids.keys(), reverse=True)
#         sorted_asks = sorted(self.asks.keys())
#         for i in range(0, self.levels): # bid price
#             if i < len(self.bids):
#                 values.append(sorted_bids[i])
#             else:
#                 values.append(0)
#         for i in range(0, self.levels): # ask price
#             if i < len(self.asks):
#                 values.append(sorted_asks[i])
#             else:
#                 values.append(0)
#         for i in range(0, self.levels): # bid depth
#             if i < len(self.bids):
#                 values.append(self.bids[sorted_bids[i]])
#             else:
#                 values.append(0)
#         for i in range(0, self.levels): # ask depth
#             if i < len(self.asks):
#                 values.append(self.asks[sorted_asks[i]])
#             else:
#                 values.append(0)
#         return values
#
#     def to_array(self):
#         '''Return Order as numpy array.'''
#         values = []
#         values.append(int(self.sec))
#         values.append(int(self.nano))
#         sorted_bids = sorted(self.bids.keys(), reverse=True)
#         sorted_asks = sorted(self.asks.keys())
#         for i in range(0, self.levels): # bid price
#             if i < len(self.bids):
#                 values.append(sorted_bids[i])
#             else:
#                 values.append(0)
#         for i in range(0, self.levels): # ask price
#             if i < len(self.asks):
#                 values.append(sorted_asks[i])
#             else:
#                 values.append(0)
#         for i in range(0, self.levels): # bid depth
#             if i < len(self.bids):
#                 values.append(self.bids[sorted_bids[i]])
#             else:
#                 values.append(0)
#         for i in range(0, self.levels): # ask depth
#             if i < len(self.asks):
#                 values.append(self.asks[sorted_asks[i]])
#             else:
#                 values.append(0)
#         return np.array(values)
#
#     def to_txt(self, fout):

# class Booklist():
#     """A class to store Books.
#
#     Provides methods for writing to external databases.
#
#     Examples
#     --------
#     Create a Booklist::
#
#     >> booklist = hft.BookList(['GOOG', 'AAPL'], levels=10)
#
#     Attributes
#     ----------
#     books : list
#         A list of Books
#     method : string
#         Specifies the type of database to create ('hdf5' or 'postgres')
#
#     """
#
#     def __init__(self, date, names, levels, method):
#         self.books = {}
#         self.method = method
#         for name in names:
#             self.books[name] = {'hist':[], 'cur':Book(date, name, levels)}
#
#     def update(self, message):
#         """Update Book data from message."""
#         b = self.books[message.name]['cur'].update(message)
#         if self.method == 'hdf5':
#             self.books[message.name]['hist'].append(b.to_array())
#         if self.method == 'postgres':
#             self.books[message.name]['hist'].append(b.to_list())
#
#     def to_hdf5(self, name, db):
#         """Write Book data to HDF5 file."""
#         hist = self.books[name]['hist']
#         if len(hist) > 0:
#             array = np.array(hist)
#             db_size, db_cols = db.orderbooks[name].shape  # rows
#             array_size, array_cols = array.shape
#             db_resize = db_size + array_size
#             db.orderbooks[name].resize((db_resize,db_cols))
#             db.orderbooks[name][db_size:db_resize,:] = array
#             self.books[name]['hist'] = []  # reset
#         print('wrote {} books to dataset (name={})'.format(len(hist), name))
#
#     def to_postgres(self, name, db):
#         """Write Book data to PostgreSQL database."""
#         db.open()
#         hist = self.books[name]['hist']
#         with db.conn.cursor() as cursor:
#             for book in hist:
#                 try:
#                     cursor.execute('insert into orderbooks values%s;', [tuple(book)])  # %s becomes "(x,..., x)"
#                 except pg.Error as e:
#                     print(e.pgerror)
#         db.conn.commit()
#         db.close()
#         self.books[name]['hist'] = []  # reset
#         print('wrote {} books to table (name={})'.format(len(hist),name))


def get_message_size(size_in_bytes):
    """Return number of bytes in binary message as an integer."""
    (message_size,) = struct.unpack('>H', size_in_bytes)
    return message_size

def get_message_type(type_in_bytes):
    """Return the type of a binary message as a string."""
    return type_in_bytes.decode('ascii')

def get_message(message_bytes, message_type, date, time, version):
    """Return binary message data as a Message."""
    if message_type in ('T', 'S', 'H', 'A', 'F', 'E', 'C', 'X', 'D', 'U', 'P', 'Q', 'I'):
        message = protocol(message_bytes, message_type, time, version)
        if version == 5.0:
            message.sec = int(message.nano / 10**9)
            message.nano = message.nano % 10**9
        message.date = date
        return message
    else:
        return None

def protocol(message_bytes, message_type, time, version):
    """Decode binary message data and return as a Message."""
    if message_type in ('T', 'S', 'H', 'A', 'F', 'E', 'C', 'X', 'D', 'U', 'P'):
        message = Message()
    elif message_type in ('Q', 'I'):
        message = NOIIMessage()
    # elif message_type in ('H'):
    #     message = TradingActionMessage()
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
        elif message.type == 'S':  # systems
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
            message.mpid = temp[6].decode('ascii').rstrip(' ')
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
        elif message.type == 'P':  # trade message
            temp = struct.unpack('>IQsI8sIQ', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.refno = temp[1]
            message.buysell = temp[2].decode('ascii')
            message.shares = temp[3]
            message.name = temp[4].decode('ascii').rstrip(' ')
            message.price = temp[5]
            # message.matchno = temp[6]
        elif message.type == 'I':
            temp = struct.unpack('>IQQs8sIIIss', message_bytes)
            message.sec = time
            message.nano = temp[0]
            message.paired = temp[1]
            message.imbalance = temp[2]
            message.direction = temp[3].decode('ascii')
            message.name = temp[4].decode('ascii').rstrip(' ')
            message.far = temp[5]
            message.near = temp[6]
            message.current = temp[7]
            message.cross = temp[8].decode('ascii')
            # message.pvar = temp[9].decode('ascii'])
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

def unpack(fin, ver, date, nlevels, names, method=None, fout=None, host=None, user=None):
    """Read ITCH data file, construct LOB, and write to database.

    This method reads binary data from a ITCH data file, converts it into human-readable data, then saves time series of out-going messages as well as reconstructed order book snapshots to a research database.

    The version number of the ITCH data is specified as a float. Supported versions are: 4.0, 4.1, and 5.0.

    """

    MAXROWS = 10**4
    orderlist = Orderlist()
    booklist = Booklist(date, names, nlevels, method)
    messagelist = Messagelist(date, names)

    if method == 'hdf5':
        db = Database(fout, names, nlevels)
    elif method == 'postgres':
        db = Postgres(host=host, user=user, nlevels=nlevels)
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
        message = get_message(message_bytes, message_type, date, clock, ver)
        messagecount += 1

        # update clock
        if message_type == 'T':
            if message.sec % 60 == 0:
                print('TIME={}'.format(message.sec))
                # pass
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
            input('PAUSED (press any button to continue).')

        # if message_type == 'H':
        #     # print('TRADE-ACTION MESSAGE: {}'.format(message.event))
        #     if message.event in ('H','P','V'):
        #         pass  # remove message.name from names
        #     elif message.event == 'T':
        #         pass  # add message.name to names (check that it isn't there already)
        #     elif message.event in ('Q','R'):
        #         pass  # quote only (only accepting A, F, X, D, U)

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
                print(del_message)
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
        elif message_type in ('P'):
            print(message)

        # update messages, books, and write to disk
        if method == 'hdf5':
            if message_type == 'U':
                if del_message.name in names:
                    messagelist.add(del_message)
                    # print('{} message added to list'.format(del_message.type))
                    msgs = len(messagelist.messages[del_message.name])
                    if msgs == MAXROWS:
                        messagelist.to_hdf5(name=del_message.name, db=db)
                    booklist.update(del_message)
                    # print('{} book was updated.'.format(del_message.name))
                    bks = len(booklist.books[del_message.name]['hist'])
                    if bks == MAXROWS:
                        booklist.to_hdf5(name=del_message.name, db=db)
                if add_message.name in names:
                    messagelist.add(add_message)
                    # print('{} message added to list'.format(add_message.type))
                    msgs = len(messagelist.messages[add_message.name])
                    if msgs == MAXROWS:
                        messagelist.to_hdf5(name=add_message.name, db=db)
                    booklist.update(add_message)
                    # print('{} book was updated.'.format(add_message.name))
                    bks = len(booklist.books[add_message.name]['hist'])
                    if bks == MAXROWS:
                        booklist.to_hdf5(name=add_message.name, db=db)
            elif message_type in ('A', 'F', 'E', 'C', 'X', 'D'):
                if message.name in names:
                    messagelist.add(message)
                    # print('{} message added to list'.format(message.type))
                    msgs = len(messagelist.messages[message.name])
                    if msgs == MAXROWS:
                        messagelist.to_hdf5(name=message.name, db=db)
                    booklist.update(message)
                    # print('{} book was updated.'.format(message.name))
                    bks = len(booklist.books[message.name]['hist'])
                    if bks == MAXROWS:
                        booklist.to_hdf5(name=message.name, db=db)
        elif method == 'postgres':
            if message_type == 'U':
                if del_message.name in names:
                    messagelist.add(del_message)
                    # print('{} message added to list'.format(del_message.type))
                    msgs = len(messagelist.messages[del_message.name])
                    if msgs == MAXROWS:
                        messagelist.to_postgres(name=del_message.name, db=db)
                    booklist.update(del_message)
                    # print('{} book was updated.'.format(del_message.name))
                    bks = len(booklist.books[del_message.name]['hist'])
                    if bks == MAXROWS:
                        booklist.to_postgres(name=del_message.name, db=db)
                if add_message.name in names:
                    messagelist.add(add_message)
                    # print('{} message added to list'.format(add_message.type))
                    msgs = len(messagelist.messages[add_message.name])
                    if msgs == MAXROWS:
                        messagelist.to_postgres(name=add_message.name, db=db)
                    booklist.update(add_message)
                    # print('{} book was updated.'.format(add_message.name))
                    bks = len(booklist.books[add_message.name]['hist'])
                    if bks == MAXROWS:
                        booklist.to_postgres(name=add_message.name, db=db)
            elif message_type in ('A', 'F', 'E', 'C', 'X', 'D'):
                if message.name in names:
                    messagelist.add(message)
                    # print('{} message added to list'.format(message.type))
                    msgs = len(messagelist.messages[message.name])
                    if msgs == MAXROWS:
                        messagelist.to_postgres(name=message.name, db=db)
                    booklist.update(message)
                    # print('{} book was updated.'.format(message.name))
                    bks = len(booklist.books[message.name]['hist'])
                    if bks == MAXROWS:
                        booklist.to_postgres(name=message.name, db=db)
        elif method == 'csv':
            pass

    # clean up
    for name in names:
        if method == 'hdf5':
            messagelist.to_hdf5(name=name, db=db)
            booklist.to_hdf5(name=name, db=db)
        elif method == 'postgres':
            messagelist.to_postgres(name=name, db=db)
            booklist.to_postgres(name=name, db=db)

    stop = time.time()

    data.close()
    db.close()

    print('Elapsed time: {} seconds'.format(stop - start))
    print('Messages read: {}'.format(messagecount))

def load_hdf5(db, name):
    """Read data from database and return pd.DataFrames."""
    try:
        with h5py.File(db, 'r') as f: # read, file must exist
            try:  # get message data
                messages = f['/messages/' + name]
                data = messages[:,:]
                T,N = data.shape
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
                data = f['/orderbooks/' + name]
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

def load_postgres(host, user, name, date):
    """Read data from PostgreSQL database and return pd.DataFrames."""

    msg_sql = """
    select *
    from messages
    where name = %s
    and date = %s
    order by sec, nano
    """

    book_sql = """
    select *
    from orderbooks
    where name = %s
    and date = %s
    order by sec, nano
    """

    # open connection to database
    try:
        conn = pg.connect(host=host, user=user)
        with conn.cursor() as cur:
            try:  # select message data
                cur.execute(msg_sql, [name, date])
                conn.commit()
                columns = ['date',
                           'sec',
                           'nano',
                           'type',
                           'event',
                           'name',
                           'buysell',
                           'price',
                           'shares',
                           'refno',
                           'newrefno']
                df_message = pd.DataFrame(cur.fetchall(), columns=columns)
            except pg.Error as e:
                print(e.pgerror)
            try:  # select order book data
                cur.execute(book_sql, [name, date])
                conn.commit()
                df_book = pd.DataFrame(cur.fetchall())
                nlevels = int((df_book.shape[1] - 2) / 4)
                base_columns = [str(i) for i in list(range(1, nlevels + 1))]
                info_columns = ['date', 'sec', 'nano', 'name']
                price_columns = ['bidprc.' + i for i in base_columns]
                volume_columns = ['bidvol.' + i for i in base_columns]
                price_columns.extend(['askprc.' + i for i in base_columns])
                volume_columns.extend(['askvol.' + i for i in base_columns])
                columns = info_columns + price_columns + volume_columns
                df_book.columns = columns
                df_time = df_book.loc[:, ('sec', 'nano')]
                df_price = df_book.loc[:, price_columns]
                df_volume = df_book.loc[:, volume_columns]
                df_price = pd.concat([df_time, df_price], axis=1)
                df_volume = pd.concat([df_time, df_volume], axis=1)
            except pg.Error as e:
                print(e.pgerror)
        conn.close()
        return (df_message, df_price, df_volume)
    except pg.Error as e:
        print('ERROR: unable to connect to database.')

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

def imshow(data, type):
    """
        Display order book data as an image, where order book data is either of
        `df_price` or `df_volume` returned by `load_hdf5` or `load_postgres`.
    """
    levels = int((data.shape[1] - 1) / 2)
    if type == 'prices':
        idx = ['askprc.' + str(i) for i in range(levels, 0, -1)]
        idx.extend(['bidprc.' + str(i) for i in range(1, levels + 1, 1)])
    elif type == 'volumes':
        idx = ['askvol.' + str(i) for i in range(levels, 0, -1)]
        idx.extend(['bidvol.' + str(i) for i in range(1, levels + 1, 1)])
    plt.imshow(data.loc[:,idx].T, interpolation='nearest', aspect='auto', cmap='gray')
    plt.show()

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
