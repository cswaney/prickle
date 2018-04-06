# prickle
A Python toolkit for high-frequency trade research.

## Table of Contents
1. [Overview](#overview)
2. [Nasdaq ITCH Data](#nasdaq-itch-data)
3. [Package Details](#package-details)
4. [Examples](#examples)


## Overview
Prickle is a Python package for financial researchers. It is designed to simplify the collection of ultra-high-frequency market microstructure data from Nasdaq. The project provides an open-source tool for market microstructure researchers to process Nasdaq HistoricalView-ITCH data. It provides a free alternative to paying for similar data, and establishes a common set of preprocessing steps that can be maintained and improved by the community.

Prickle creates research-ready databases from Nasdaq HistoricalView-ITCH data files. Raw ITCH files are provided "as is" in a compressed, binary format that is not particularly useful. Prickle decodes these files and creates databases containing sequences of messages, reconstructed order books, along with a few other events of interest.

## Nasdaq ITCH Data
Nasdaq HistoricalView-ITCH describes changes to Nasdaq limit order books at nanosecond precision. Researchers at academic institutions can obtain the data for free from Nasdaq by signing an academic non-disclosure agreement. Nasdaq records updates for all of their order books in a single file each day. The updates are registered as a sequence of variable-length binary “messages” that must to be decoded. Nasdaq provides documentation that gives the exact format for every message type depending on the version of the data.

The messages all follow a specific format:
1. First, a (two-byte) integer specifies the number of bytes in the message.
2. Next, a (one-byte) character specifies the **type** of the message.
3. Finally, the message is a sequence of bytes whose format depends on its type.

In Python, it is simple to decode the message bytes using the built-in `struct.unpack` method. (In fact, it seems that passing the number of bytes is unnecessary because every message type has a fixed number of bytes, so knowing the type of the message should be enough).

The logic behind this format is clear enough: storing messages this way saves a lot of space. From the researchers point of view, however, there are serious problems. First, the messages need to be decoded before they are human-readable. Second, because of the sequential nature of the messages—and the fact that all stocks are combined into a single daily file—there is no choice but to read (and decode) *every* message. There hundreds of millions of messages each day. The goal of prickle is to convert the daily message files into databases that are user-friendly.

Here are a few examples. The simplest message is a timestamp message that is used to keep track of time. A timestamp message looks like this (after decoding bytes):

`(size=5, type=T, sec=22149)`

This happens to be the first message of the day. The next message is

`(size=6, type=S, nano=287926965, event=O)`

The type 'S' means that it is a system message; it occurred 287926965 nanoseconds after the first message, and the event type 'O' indicates that it marks the beginning of the recording period.

If we keep reading messages, we will eventually come to one that look like this:

`(size=30, type=A, nano=163877100, name=ACAS, size=B, price=115000, shares=500, refno=26180)`

This is the first true order book event in the file. It indicates that a bid (limit order) was placed for 500 shares of ACAS stock at $115.000, and that the order arrived 163877100 nanoseconds after the last second. Notice also that the message contains a reference number (26180): the reference number is important because it allows us to keep track of the status of each order. For example, after this order was placed, we might see another message that looks like this:

`(size=13, type=D, nano=552367044, refno=26180)`

This message says that the order with reference number 26180 was deleted, and we can therefore remove those shares from the order book and stop keeping track of the order's status.

Besides the messages, researchers may be interested in the actual order book. The messages only indicate changes to the order book, but prickle uses the changes to reconstruct the state of the order book.

## Package Details
The main method of prickle is `unpack`, which processes daily ITCH message files (one at a time). `unpack` is not intended to process the messages for all of the securities traded on Nasdaq in one pass. Rather, it is expected that the user provides a list of stocks that she would like data for (which could range from a single stock to several hundred—`unpack` has no problem to process the S&P 500). Focusing on a smaller list of stocks allows us to “skip” messages. It also helps alleviate the tension between writing messages to file and storing messages in memory. It is inefficient to write single messages to file, but storing the processed data in local memory is also infeasible (for a decent number of securities). Therefore, `unpack` stores the processed messages up to a buffer size before writing the messages to file. You can determine/fix the maximum amount of memory that `unpack` will use ahead of time by adjusting the buffer size. For example, if you only want to process data for a single stock, then no buffer is required. If you want to process data for the entire S&P 500, and you only have say 2GB of memory available, then you could select a buffer size around 10,000 messages. (What is the approximate calculation?)

Processing data for a few hundred stocks might take several hours. If you intend to process several months or years of data, then you will probably want to run the jobs on a cluster, in which case you might face memory constraints on each compute node. The buffer size allows you to fix the maximum memory required in advance. Note as well that there is not much benefit to increasing buffer sizes beyond a certain point because 100,000 messages per day is relative large number, but only amounts to 10 writes (at a 10,000 buffer a size).

In addition to processing the binary messages, prickle generates reconstructed order books. The process for doing so centers around the nature of the message data. In particular, Nasdaq reduces the amount of data passed directly by each message by using reference numbers on orders that update earlier orders. For example, if the original order specified (type=‘A’, name=’AAPL’, price=135.00, shares=100, refno=123456789), then a subsequent message informing market participants that the order was executed would look something like this: (type=‘E’, shares=100, refno=123456789). Therefore, instead of simply using each order to directly make changes to the order book, `unpack` maintains a list of outstanding orders that it uses to keep track of the current state of each order, and fill-in missing data from incoming messages that can then be used to make updates to order books. The complete flow of events is shown in the figure below.

![unpack flow chart]()


As you can see, `unpack` generates (or updates) five databases: NOII messages, system messages, trade messages, messages, and (order) books.

1. **NOII Messages**: net order imbalances and crossing messages.
2. **System Messages**: critical system-wide information (e.g., start of trading).
3. **Trade Messages**: indicate trades against hidden (non-displayed) liquidity.
4. **Messages**: all other messages related to order book updates.
5. **Books**: snapshots of limit order books following each update.

Finally, `unpack` provides two methods for storing the processed data.

1. **CSV**: The simplest choice is to store the data in csv files, organized by type, date, and security name. The organization is natural for research intending to perform analysis at the stock-day level. This choice is similar to the HDF5 choice in terms of organization and workflow, but loading data is considerably slower. In addition, the entire stock-day file must be loaded into memory before any slicing can be applied. A benefit of this format is that the data is stored in an easily interpreted manner.
2. **HDF5**: HDF5 is a popular choice for storing scientific data. With this option, data is organized by day, security, and type. It is therefore intended to be handled on a stock-day basis. Loading message or order book data for a single stock on a single day is extremely fast. The downside is that data is stored as a single data type (integers). Therefore, some of the data is not directly interpretable (e.g., the message types). In contrast to csv files, HDF5 files can be sliced *before* loading data into Python.

## Examples

## Installation
![alt text](https://img.shields.io/pypi/v/hfttools.svg "pypi")

### Requirements
This package runs on **Python3.5**. By default, the data is stored in text/comma-separated files. You will need the following to create HDF5 or PostgreSQL databases (these are **not** installed automatically):

1. [HDF5](https://www.hdfgroup.org).
2. [PostgreSQL](https://www.postgresql.org)

After you have installed and configured these, simply install using the Python package manager. We recommend using a virtual environment:

```
virtualenv -p python3 venv
source venv/bin/activate
pip install hfttools
```

## Basic Usage
To create a new HDF5 database from an ITCH data file `itch_010113`:

```python
import hfttools as hft

hft.unpack(fin='itch_010113.bin',
		   ver=4.1,
		   date='2013-01-01',
		   fout='itch.hdf5'
		   nlevels=10,
		   names=['GOOG', 'AAPL'],
		   method='hdf5')
```

This will create a file `itch.hdf5` containing message and order book data for Google and Apple. To read the order book data back into your Python session, use `hft.read`:

```python
hft.read(db='itch.hdf5',
		 date='2013-01-01',
		 names='GOOG')
```

For more information, see the tutorial at the projects [webpage](https://cswaney.github.io/hfttools/).

## Tips
Create massive datasets quickly by running jobs simultaneously (e.g. on your university's cluster). All databases support simultaneous read/write.  

## License
This package is released under an MIT license. Please cite me (e.g. HFTTools (Version 0.0.2, 2016)).
