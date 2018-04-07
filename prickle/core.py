import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
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

    def __init__(self, path, names, nlevels, method):
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
                if name in self.file['trades'].keys():
                    print('Overwriting trades data for {}'.format(name))
                    del self.file['trades'][name]
                if name in self.file['noii'].keys():
                    print('Overwriting noii data for {}'.format(name))
                    del self.file['noii'][name]
        except OSError as e:
            print('HDF5 file does not exist. Creating a new one.')
            self.file = h5py.File(path, 'x')  # create file, fail if exists
        # open groups, create otherwise
        self.messages = self.file.require_group('messages')
        self.orderbooks = self.file.require_group('orderbooks')
        self.trades = self.file.require_group('trades')
        self.noii = self.file.require_group('noii')
        # open datasets, create otherwise
        for name in names:
            self.messages.require_dataset(name,
                                          shape=(0, 8),
                                          maxshape=(None,None),
                                          dtype='i')
            self.orderbooks.require_dataset(name,
                                            shape=(0, 4 * nlevels + 2),
                                            maxshape=(None,None),
                                            dtype='i')
            self.trades.require_dataset(name,
                                        shape=(0, 5),
                                        maxshape=(None,None),
                                        dtype='i')
            self.noii.require_dataset(name,
                                      shape=(0, 14),
                                      maxshape=(None,None),
                                      dtype='i')

    def close(self):
        self.file.close()

# TODO: Make the different types of messages sub-classes of Message?
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
        assert self.type == 'U', "ASSERT-ERROR: split method called on non-replacement message."
        if self.type == 'U':
            new_message = Message(date=self.date,
                                  sec=self.sec,
                                  nano=self.nano,
                                  type='U',
                                  price=self.price,
                                  shares=self.shares,
                                  refno=self.refno,
                                  newrefno=self.newrefno)
            del_message = Message(date=self.date,
                                  sec=self.sec,
                                  nano=self.nano,
                                  type='D',
                                  refno=self.refno,
                                  newrefno=-1)
            add_message = Message(date=self.date,
                                  sec=self.sec,
                                  nano=self.nano,
                                  type='U+',
                                  price=self.price,
                                  shares=self.shares,
                                  refno=self.refno,
                                  newrefno=self.newrefno)
            return (new_message, del_message, add_message)

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

        # if self.type in ('A', 'F', 'X', 'D', 'E', 'C', 'U'):
        #     sec = self.sec
        #     nano = self.nano
        #     if self.side == 'B':
        #         side = -1
        #     else:
        #         side = 1
        #     price = self.price
        #     shares = self.shares
        #     values = [sec, nano, side, price, shares]

        if self.type == 'P':
            if self.buysell == 'B':
                side = -1
            else:
                side = 1
            values = [self.sec, self.nano, side, self.price, self.shares]
            return np.array(values)

        # type
        if self.type == 'A':  # add
            values.append(0)
        elif self.type == 'F':  # add w/mpid
            values.append(1)
        elif self.type == 'X':  # cancel
            values.append(2)
        elif self.type == 'D':  # delete
            values.append(3)
        elif self.type == 'E':  # execute
            values.append(4)
        elif self.type == 'C':  # execute w/price
            values.append(5)
        elif self.type == 'U':  # replace
            values.append(6)
        else:
            value.append(-1)

        # side
        if self.buysell == 'B':  # bid
            values.append(1)
        elif self.buysell == 'S':  # ask
            values.append(-1)
        else:
            values.append(0)

        # price
        values.append(int(self.price))

        # shares
        values.append(int(self.shares))

        # refno
        if self.type in ('A', 'F', 'X', 'D', 'E', 'C', 'U'):
            values.append(int(self.refno))
        else:
            values.append(-1)

        # newrefno
        if self.type == 'U':
            values.append(int(self.newrefno))
        else:
            values.append(-1)

        return np.array(values)

    def to_txt(self, path=None):
        if self.type in ('S', 'H'):
            sep = ','
            line = [str(self.sec),
                    str(self.nano),
                    str(self.name),
                    str(self.event)]
        elif self.type in ('A', 'F', 'E', 'C', 'X', 'D', 'U'):
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
        elif self.type == 'P':
            sep = ','
            line = [str(self.sec),
                    str(self.nano),
                    str(self.name),
                    str(self.buysell),
                    str(self.shares),
                    str(self.price / 10 ** 4)]
        if path is None:
            return sep.join(line) + '\n'
        else:
            with open(path, 'a') as fout:
                fout.write(sep.join(line) + '\n')

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

        if self.type == 'Q':  # cross trade
            type = 0
        elif self.type == 'I':  # noii
            type = 1
        else:
            type = -1
            print('Unexpected NOII message type: {}'.format(self.type))

        if self.cross == 'O':    # opening cross
            cross = 0
        elif self.cross == 'C':  # closing cross
            cross = 1
        elif self.cross == 'H':  # halted cross
            cross = 2
        elif self.cross == 'I':  # intraday cross
            cross = 3
        else:
            cross = -1
            print('Unexpected cross type: {}'.format(self.cross))

        if self.buysell == 'B':  # bid
            side = 1
        elif self.buysell == 'S':  # ask
            side = -1
        else:
            side = 0

        if self.direction == 'B':  # bid
            dir = 1
        elif self.direction == 'S':  # ask
            dir = -1
        else:
            dir = 0

        values =[self.sec,
                 self.nano,
                 type,
                 cross,
                 side,
                 self.price,
                 self.shares,
                 self.matchno,
                 self.paired,
                 self.imbalance,
                 dir,
                 self.far,
                 self.near,
                 self.current]
        return np.array(values)

    def to_txt(self, path):
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
        with open(path, 'a') as fout:
            fout.write(sep.join(line) + '\n')

class Trade():
    """A class representing trades on the NASDAQ system.

    Parameters
    ----------
    date: int
        Date
    sec : int
        Seconds
    nano : int
        Nanoseconds
    name : string
        Stock ticker
    side : string
        Buy or sell
    price : int
        Trade price
    shares : int
        Shares
    """

    def __init__(self, date='.', sec=-1, nano=-1, name='.', side='.', price=-1, shares=0):
        self.date = date
        self.name = name
        self.sec = sec
        self.nano = nano
        self.side = side
        self.price = price
        self.shares = shares

    def __str__(self):
        sep = ', '
        line = ['sec: ' + str(self.sec),
                'nano: ' + str(self.nano),
                'name: ' + str(self.name),
                'side: ' + str(self.buysell),
                'price: ' + str(self.price),
                'shares: ' + str(self.shares)]
        return sep.join(line)

    def __repr__(self):
        sep = ', '
        line = ['sec: ' + str(self.sec),
                'nano: ' + str(self.nano),
                'name: ' + str(self.name),
                'side: ' + str(self.buysell),
                'price: ' + str(self.price),
                'shares: ' + str(self.shares)]
        return 'Trade(' + sep.join(line) + ')'

    def to_list(self):
        """Returns message as a list."""

        values = []
        values.append(str(self.date))
        values.append(str(self.name))
        values.append(int(self.sec))
        values.append(int(self.nano))
        values.append(str(self.side))
        values.append(int(self.price))
        values.append(int(self.shares))
        return values

    def to_array(self):
        """Returns message as an np.array of integers."""

        if self.side == 'B':
            side = -1
        else:
            side = 1
        return np.array([self.sec, self.nano, side, self.price, self.shares])

    def to_txt(self, path=None):
        sep = ','
        line = [str(self.sec),
                str(self.nano),
                str(self.name),
                str(self.side),
                str(self.shares),
                str(self.price / 10 ** 4)]
        if path is None:
            return sep.join(line) + '\n'
        else:
            with open(path, 'a') as fout:
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

    >> msglist = pk.Messagelist(date='112013', names=['GOOG', 'AAPL'])

    """

    def __init__(self, date, names):
        self.messages = {}
        self.date = date
        for name in names:
            self.messages[name] = []

    def add(self, message):
        """Add a message to the list."""
        try:
            self.messages[message.name].append(message)
        except KeyError as e:
            print("KeyError: Could not find {} in the message list".format(message.name))

    def to_hdf5(self, name, db, grp):
        """Write messages to HDF5 file."""
        m = self.messages[name]
        if len(m) > 0:
            listed = [message.to_array() for message in m]
            array = np.array(listed)
            if grp == 'messages':
                db_size, db_cols = db.messages[name].shape  # rows
                array_size, array_cols = array.shape
                db_resize = db_size + array_size
                db.messages[name].resize((db_resize, db_cols))
                db.messages[name][db_size:db_resize, :] = array
            if grp == 'trades':
                db_size, db_cols = db.trades[name].shape  # rows
                array_size, array_cols = array.shape
                db_resize = db_size + array_size
                db.trades[name].resize((db_resize, db_cols))
                db.trades[name][db_size:db_resize, :] = array
            if grp == 'noii':
                db_size, db_cols = db.noii[name].shape  # rows
                array_size, array_cols = array.shape
                db_resize = db_size + array_size
                db.noii[name].resize((db_resize, db_cols))
                db.noii[name][db_size:db_resize, :] = array
            self.messages[name] = []  # reset
        print('wrote {} messages to database (name={}, group={})'.format(len(m), name, grp))

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
            if message.type == 'U':
                message.name = ref_order.name
                message.buysell = ref_order.buysell
            elif message.type == 'U+':  # ADD from a split REPLACE order
                message.type = 'A'
                message.name = ref_order.name
                message.buysell = ref_order.buysell
                message.refno = message.newrefno
                message.newrefno = -1
            elif message.type in ('E', 'C', 'X'):
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
        """Update Book using incoming Message data."""
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

    def to_txt(self):
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
        return ','.join([str(v) for v in values]) + '\n'
        # with open(path, 'a') as fout:
        #     fout.write(','.join([str(v) for v in values]) + '\n')

class Booklist():
    """A class to store Books.

    Provides methods for writing to external databases.

    Examples
    --------
    Create a Booklist::

    >> booklist = pk.BookList(['GOOG', 'AAPL'], levels=10)

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
        elif message.type == 'F':  # add w/mpid
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
            message.name = '.'
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

    The version number of the ITCH data is specified as a float. Supported versions are: 4.1.

    """

    MAXROWS = 10**4
    orderlist = Orderlist()
    booklist = Booklist(date, names, nlevels, method)
    messagelist = Messagelist(date, names)

    if method == 'hdf5':
        db = Database(fout, names, nlevels)  # TODO: method='hdf5'
    elif method == 'csv':
        # TODO: db = Database(fout, names, nlevels, method='csv')
        pass
    else:
        print('No database option specified. Creating csv files.')

    data = open(fin, 'rb')
    messagecount = 0
    reading = True
    # writing = False
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
            if message.sec % 1800 == 0:
                print('TIME={}'.format(message.sec))
            clock = message.sec

        # update system
        if message_type == 'S':
            print('SYSTEM MESSAGE: {}'.format(message.event))
            # TODO: write to log file! (message.to_log(log_path))
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
        if message_type == 'H':
            if message.name in names:
                # print('TRADING MESSAGE ({}): {}'.format(message.name, message.event))
                # TODO: write to log (message.to_log('system.log')
                if message.event == 'H':  # halted (all US)
                    pass
                elif message.event == 'P':  # paused (all US)
                    pass
                elif message.event == 'Q':  # quotation only
                    pass
                elif message.event == 'T':  # trading on nasdaq
                    pass

        # complete message
        if message_type == 'U':
            message, del_message, add_message = message.split()
            orderlist.complete_message(message)
            orderlist.complete_message(del_message)
            orderlist.complete_message(add_message)
            if message.name in names:
                orderlist.update(del_message)
                booklist.update(del_message)
                orderlist.add(add_message)
                booklist.update(add_message)
                messagelist.add(message)
        elif message_type in ('E', 'C', 'X', 'D'):
            orderlist.complete_message(message)
            if message.name in names:
                orderlist.update(message)
                booklist.update(message)
                messagelist.add(message)
        elif message_type in ('A', 'F'):
            if message.name in names:
                orderlist.add(message)
                booklist.update(message)
                messagelist.add(message)
        # elif message_type == 'P':  # TODO
        #     if message.name in names:
        #         tradeslist.add(message)
        # elif message_type in ('Q', 'I'):
        #     if message.name in names:
        #         noiilist.add(message)

        # write
        if method == 'hdf5':
            if message_type in ('U', 'A', 'F', 'E', 'C', 'X', 'D'):
                if message.name in names:
                    # messagelist.add(message)
                    if len(messagelist.messages[message.name]) == MAXROWS:
                        messagelist.to_hdf5(name=message.name, db=db)
                    # booklist.update(message)
                    if len(booklist.books[message.name]['hist']) == MAXROWS:
                        booklist.to_hdf5(name=message.name, db=db)
            elif message_type == 'P':
                if message.name in names:
                    if len(tradeslist.trades[message.name]) == MAXROWS:
                        tradeslist.to_hdf5(name=message.name, db=db)
            elif message_type in ('Q', 'I'):
                if message.name in names:
                    if len(noiilist.messages[message.name]) == MAXROWS:
                        noiilist.to_hdf5(name=message.name, db=db)
        elif method == 'csv':
            pass

    # clean up
    for name in names:
        if method == 'hdf5':
            messagelist.to_hdf5(name=name, db=db)
            booklist.to_hdf5(name=name, db=db)
            # TODO:
            # tradeslist.to_hdf5(name=name, db=db)
            # noiilist.to_hdf5(name=name, db=db)
        elif method == 'csv':
            pass

    stop = time.time()

    data.close()
    db.close()

    print('Elapsed time: {} seconds'.format(stop - start))
    print('Messages read: {}'.format(messagecount))

def load_hdf5(db, name, grp):
    """Read data from database and return pd.DataFrames."""

    if grp == 'messages':
        try:
            with h5py.File(db, 'r') as f:
                try:
                    messages = f['/messages/' + name]
                    data = messages[:,:]
                    T,N = data.shape
                    columns = ['sec',
                               'nano',
                               'type',
                               'side',
                               'price',
                               'shares',
                               'refno',
                               'newrefno']
                    df = pd.DataFrame(data, index=np.arange(0,T), columns=columns)
                    return df
                except KeyError as e:
                    print('Could not find name {} in messages'.format(name))
        except OSError as e:
            print('Could not find file {}'.format(path))

    if grp == 'books':
        try:
            with h5py.File(db, 'r') as f:
                try:
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
                    return df_price, df_volume
                except KeyError as e:
                    print('Could not find name {} in orderbooks'.format(name))
        except OSError as e:
            print('Could not find file {}'.format(path))

    if grp == 'trades':
        try:
            with h5py.File(db, 'r') as f:
                try:
                    messages = f['/trades/' + name]
                    data = messages[:,:]
                    T,N = data.shape
                    columns = ['sec',
                               'nano',
                               'side',
                               'price',
                               'shares']
                    df = pd.DataFrame(data, index=np.arange(0,T), columns=columns)
                    return df
                except KeyError as e:
                    print('Could not find name {} in messages'.format(name))
        except OSError as e:
            print('Could not find file {}'.format(path))

    if grp == 'noii':
        try:
            with h5py.File(db, 'r') as f:
                try:
                    messages = f['/noii/' + name]
                    data = messages[:,:]
                    T,N = data.shape
                    columns = ['sec',
                               'nano',
                               'type',
                               'cross',
                               'side',
                               'price',
                               'shares',
                               'matchno',
                               'paired',
                               'imb',
                               'dir',
                               'far',
                               'near',
                               'current']
                    df = pd.DataFrame(data, index=np.arange(0,T), columns=columns)
                    return df
                except KeyError as e:
                    print('Could not find name {} in messages'.format(name))
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

def imshow(data, which, levels):
    """
        Display order book data as an image, where order book data is either of
        `df_price` or `df_volume` returned by `load_hdf5` or `load_postgres`.
    """

    if which == 'prices':
        idx = ['askprc' + str(i) for i in range(levels-1, -1, -1)]
        idx.extend(['bidprc' + str(i) for i in range(0, levels, 1)])
    elif which == 'volumes':
        idx = ['askvol' + str(i) for i in range(levels-1, -1, -1)]
        idx.extend(['bidvol' + str(i) for i in range(0, levels, 1)])
    plt.imshow(data.loc[:,idx].T, interpolation='nearest', aspect='auto')
    plt.yticks(range(0, levels * 2, 1), idx)
    plt.colorbar()
    plt.tight_layout()
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

def find_trades(messages, eps=10 ** -6):
    if 'time' not in messages.columns:
        messages['time'] = messages['sec'] + messages['nano'] / 10 ** 9
    if 'type' in messages.columns:
        messages = messages[messages.type == 'E']
    trades = []
    i = 0
    while i < len(messages):
        time = messages.iloc[i].time
        side = messages.iloc[i].side
        shares = messages.iloc[i].shares
        vwap = messages.iloc[i].price
        hit = 0
        i += 1
        if i == len(messages):
            break
        while messages.iloc[i].time <= time + eps and messages.iloc[i].side == side:
            shares += messages.iloc[i].shares
            if messages.iloc[i].price != vwap:
                hit = 1
                vwap = messages.iloc[i].price * messages.iloc[i].shares / shares + vwap * (shares - messages.iloc[i].shares) / shares
            i += 1
            if i == len(messages):
                break
        # print('TRADE (time={}, side={}, shares={}, vwap={}, hit={})'.format(time, side, shares, vwap, hit))
        trades.append([time, side, shares, vwap, hit])
    return pd.DataFrame(trades, columns=['time', 'side', 'shares', 'vwap', 'hit'])

def plot_trades(trades):
    sells = trades[trades.side == 'B']
    buys = trades[trades.side == 'S']
    plt.hist(sells.shares, bins=np.arange(-1000, 100, 100), edgecolor='white', color='C0', alpha=0.5)
    plt.hist(-buys.shares, bins=np.arange(1, 1100, 100), edgecolor='white', color='C1', alpha=0.5)
    plt.show()
    plt.clf()

def analyze(messages):
    # message counts
    message_counts = pd.value_counts(messages['type'])

def nodups(books, messages):
    """Return messages and books with rows remove for orders that didn't change book."""
    assert books.shape[0] == messages.shape[0], "books and messages do not have the same number of rows"
    subset = books.columns.drop(['sec', 'nano', 'name'])
    dups = books.duplicated(subset=subset)
    return books[~dups].reset_index(), messages[~dups].reset_index()

def combine(messages, hidden):
    """Combine hidden executions with message data."""
    messages = messages.drop(['index', 'sec', 'nano', 'name', 'refno', 'mpid'], axis=1)
    hidden['type'] = 'H'
    hidden = hidden.drop(['hit'], axis=1)
    hidden = hidden.rename(columns={'vwap': 'price'})
    combined = pd.concat([messages, hidden])
    return combined.sort_values(by='time', axis=0)
