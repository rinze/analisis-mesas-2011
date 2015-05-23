#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import codecs
import cStringIO
import os
from collections import namedtuple

def getParties(parties_file):
    """
    Return the parties along with their numerical code as a
    hashtable (key => code, value => party)
    """
    f = open(parties_file)

    res = dict()
    for line in f:
        # Assign fields of fixed length according to the documentation.
        code = line[8:14]
        party = unicode(line[64:214].strip(), "iso-8859-1")
        res[code] = party

    f.close()
    return(res)

def getTowns(towns_file):
    """
    Return a dictionary with the codes for each town along with their names (key => code, value => town).
    The code is a tuple containing (Province, Town) codes.
    """

    f = open(towns_file)
    reader = csv.reader(f)
    reader.next() # Skip first line, header
    res = dict()

    for entry in reader: # Skip first line, header
        code = (entry[0], entry[1]) # (Province, Town)
        town = unicode(entry[3], "utf-8")
        if code not in res:
            res[code] = town

    f.close()
    return(res)

def getElectionResults(election_file, parties_dict, towns_dict):
    """
    Return elections results at the level of individual urn. Return a list of
    namedtuples with the following fields (see code -- first line).
    """
    ElectionEntry = namedtuple('ElectionEntry', 'prov_code, town_code,' + \
                               'dist_code, section_code,' + \
                               'table_code, party_code, ' + \
                               'town_name, party_name, votes')
    f = open(election_file)
    def parseLine(line):
        # As in getParties(), parse according to official field description.
        prov_code = line[11:13]
        town_code = line[13:16]
        dist_code = line[16:18]
        section_code = line[18:22]
        table_code = line[22:23]
        party_code = line[23:29]
        votes = line[29:36]
        
        entry = ElectionEntry(prov_code, town_code, dist_code, \
                              section_code, table_code, \
                              party_code, towns_dict[(prov_code, town_code)], \
                              parties_dict[party_code], votes)
        return(entry)

    res = map(parseLine, f)
    return(res)

# From https://docs.python.org/2/library/csv.html#examples
class UnicodeWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

#### Main ####
if (__name__ == "__main__"):
    # Hardcoded file paths. Change it to use your own data. See README.md 
    # for info on each file.
    parties_file = "/home/chema/Dropbox/data/elecciones20112015/04201105_MESA/03041105.DAT"
    elections_file = "/home/chema/Dropbox/data/elecciones20112015/04201105_MESA/10041105.DAT"
    towns_file = "/home/chema/Dropbox/data/elecciones20112015/11codmun.csv"
    parties = getParties(parties_file)
    towns = getTowns(towns_file)
    results = getElectionResults(elections_file, parties, towns)
    # Don't want parties with 0 votes.
    results = [x for x in results if x.votes > 0]

    # Save as CSV for analysis with R.
    if not os.path.isdir('data'):
        os.mkdir('data')

    f = open('data/elections2011.csv', 'w')
    csv_writer = UnicodeWriter(f)
    csv_writer.writerow(results[0]._fields)
    csv_writer.writerows(results)
    f.close()
