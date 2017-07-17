import core as hft
import datetime
import sys

with open('../names.txt', 'r') as fin:
    names = [n.rstrip("\n") for n in fin.readlines()]

_, date = sys.argv
fin = '/Volumes/datasets/ITCH/bin/S{}-v41.txt'.format(date)
fout = '/Volumes/datasets/ITCH/hdf5/itch-{}.hdf5'.format(date)
# fin = '/Volumes/Data Backup/ITCH/bin/S{}-v41.txt'.format(date)
# fout = '/Volumes/Data Backup/ITCH/hdf5/itch-{}.hdf5'.format(date)
ver = 4.1
nlevels = 10
method = 'hdf5'

def stringToDate(datestring):
    return datetime.datetime.strptime(date, "%m%d%y").date()

print("Unpacking data for date {}".format(stringToDate(date)))

hft.unpack(fin, ver, date, nlevels, names, method='hdf5', fout=fout)
