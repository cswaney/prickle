# hfttools
A Python toolkit for high-frequency trade research.

Website: [https://cswaney.github.io/hfttools/](https://cswaney.github.io/hfttools/)

## What?
HFT Tools is a Python toolkit for financial researchers. It is designed to make data collection simple.

## Why?
The goal of this project is to provide a common, open-source tool for market microstucture research using NASDAQ HistoricalView-ITCH data. Don't pay for data!

## Sure. But what does is actually do?
HFT Tools creates scalable, research-ready databases from NASDAQ HistoricalView-ITCH data files. These data files are provided "as is" in a compressed, binary format that is not particularlyl useful. HFT Tools decodes these files and creates tables containing the time series of messages as well as the time series of reconstructed order books.  

## Installation
![alt text](https://img.shields.io/pypi/v/hfttools.svg "pypi")

### Requirements

This package runs on **Python3.5**. You will also need the following to create databases (these are **not** installed automatically):

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

For more information, see our tutorial at the projects [webpage](https://www.google.com).

## Tips

Create massive datasets quickly by running jobs simultaneously (e.g. on your university's cluster). All databases support simultaneous read/write.  

## License

This package is released under an MIT license. Please cite me (e.g. HFTTools (Version 0.0.2, 2016)). 
