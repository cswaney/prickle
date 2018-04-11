from prickle import unpack
import datetime
import sys
import time

_, test = sys.argv
names = ['AAPL']
date = '112013'
ver = 4.1
nlevels = 5

# hdf5
if test == 'hdf5':
    print("Unpacking data for date {}".format(date))
    fin = '/Volumes/datasets/ITCH/bin/S{}-v41.txt'.format(date)
    fout = '/Users/colinswaney/Desktop/S{}-v41.hdf5'.format(date)
    unpack(fin, ver, date, nlevels, names, method='hdf5', fout=fout)

# csv
if test == 'csv':
    print("Unpacking data for date {}".format(date))
    fin = '/Volumes/datasets/ITCH/bin/S{}-v41.txt'.format(date)
    fout = '/Users/colinswaney/Desktop/S{}-v41/csv/'.format(date)
    unpack(fin, ver, date, nlevels, names, method='csv', fout=fout)
