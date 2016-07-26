# hfttools
A Python toolkit for high-frequency trade research.

Website: [https://cswaney.github.io/hfttools/](https://cswaney.github.io/hfttools/)


## Summary

hfttools is a Python toolkit for financial researchers. Its goal is to provide a common tool for market microstucture research using NASDAQ HistoricalView-ITCH data.

## Interface

``unpack()``
    Create a database of message and order book data.

## Installation
![alt text](https://img.shields.io/pypi/v/hfttools.svg "pypi")

Using the Python package manager:

```
pip install hfttools
```

Create a new database in Python:

```
import hfttools

unpack()
```


## TODO:

(1) Add support for trading halts (global).
  - Process trading halts in System messages.
  - If halt occurs, then writing = False.
  - If resume occurs, then writing = True.
(2) Add support for trading halts (individual).
  - Process the trading halts in Stock-Action messages.
  - Keep a list of halted stocks.
  - If halt occurs, do not proceed past message completion.
  - An elegant way to deal with this is to remove the stock from the namelist
    when a halt occurs, and then append it back when it resumes.
(3) Write print statements to log file at fout location.
  - Make this an option (e.g. log=True).
