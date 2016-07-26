# hfttools
A Python toolkit for high-frequency trade research.

Website: [https://cswaney.github.io/hfttools/](https://cswaney.github.io/hfttools/)


TODO:

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
