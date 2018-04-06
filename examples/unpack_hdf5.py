from prickle import unpack
import datetime
import sys
import time

# with open('../names.txt', 'r') as fin:
#     names = [n.rstrip("\n") for n in fin.readlines()]
# _, date = sys.argv
# fin = '/Volumes/datasets/ITCH/bin/S{}-v41.txt'.format(date)
# fout = '/Volumes/datasets/ITCH/hdf5/itch-{}.hdf5'.format(date)
# ver = 4.1
# nlevels = 10
# method = 'hdf5'

names = ['AAPL', 'GOOG']
date = '112013'
fin = '/Users/colinswaney/Data/ITCH/bin/S{}-v41.txt'.format(date)
fout = '/Users/colinswaney/Data/ITCH/hdf5/itch-{}.hdf5'.format(date)
ver = 4.1
nlevels = 5
method = 'hdf5'

# def stringToDate(datestring):
#     return datetime.datetime.strptime(date, "%m%d%y").date()

print("Unpacking data for date {}".format(date))

start = time.time()
unpack(fin, ver, date, nlevels, names, method='hdf5', fout=fout)
stop = time.time()
print("Process took {} seconds".format(stop - start))
