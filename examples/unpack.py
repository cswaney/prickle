import hfttools as hft
import datetime
import sys

names = ['GOOG']
date = '070113'
fin = '/Volumes/datasets/ITCH/bin/S{}-v41.txt'.format(date)
fout = '/Users/colinswaney/Desktop/itch-{}.hdf5'.format(date)
ver = 4.1
nlevels = 20
method = 'hdf5'

hft.unpack(fin, ver, date, nlevels, names, method='hdf5', fout=fout)
