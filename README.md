# prickle
A Python toolkit for high-frequency trade research.

Website: [https://cswaney.github.io/hfttools/](https://cswaney.github.io/hfttools/)

## Overview
Prickle is a Python package for financial researchers. It is designed to simplify the collection of ultra-high-frequency market microstructure data from Nasdaq. The project provides an open-source tool for market microstucture researchers to process Nasdaq HistoricalView-ITCH data. It provides a free alternative to paying for similar data, and establishes a common set of preprocessing steps that can be maintained and improved by the community.

Prickle creates research-ready databases from Nasdaq HistoricalView-ITCH data files. Raw ITCH files are provided "as is" in a compressed, binary format that is not particularly useful. Prickle decodes these files and creates databases containing sequences of messages, reconstructed order books, along with a few other events of interest.

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
