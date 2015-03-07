#!/usr/bin/env python
#
# Module to learn cassandra interface / data model
#
#


# Datastax Cassandra client
# pip install cassandra-driver

# sh cqlsh
# CREATE KEYSPACE mykeyspace WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};
# DESCRIBE KEYSPACES
# USE mykeyspace

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

# by default connects to localhost
cluster = Cluster()

# create a session - connect to a keyspace
session = cluster.connect('prash')

# NB can also use session.set_keyspace('') OR session.execute('USE prash')

# Create an example table
session.execute("""CREATE TABLE IF NOT EXISTS prash.test(
                        doc_id varchar PRIMARY KEY,
                        url varchar,
                        final_url varchar,
                        run_id varchar,
                        doubleclick_fl varchar,
                        new_relic varchar,
                        comscore varchar,
                        facebook_li varchar,
                        crazy_egg varchar,
                        omniture varchar,
                        mediamath varchar);
                """)

rows = session.execute('SELECT * from test')
print rows

import csv

print "reading data..."
import time
t1 = time.time()
reader = csv.reader(open('/var/data/crawl_parsers/tmp/feb2015_customeranalytics_10595.csv', 'r'))
header = reader.next()
rows = [row for row in reader]
time_to_read = time.time() - t1

print "number of rows = ", len(rows), " took: ", time_to_read


prep_statement = session.prepare("""INSERT INTO prash.test (url, final_url, doc_id, run_id, doubleclick_fl, new_relic, comscore, facebook_li, crazy_egg, omniture, mediamath) VALUES (?,?,?,?,?,?,?,?,?,?,?)""")


basic_insert = """INSERT INTO prash.test (url, final_url, doc_id, run_id, doubleclick_fl, new_relic, comscore, facebook_li, crazy_egg, omniture, mediamath) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""


def batch(rows, batchsize=1):
    row_len = len(rows)
    for idx in range(0, row_len, batchsize):
        yield rows[idx:min(idx+batchsize, row_len)]


def test_batch_insert(rows):
    """Test insertion using batch / prepared statements."""

    print "inserting data"
    t1 = time.time()
    batch_gen = batch(rows, batchsize=1000)
    for batch in batch_gen:
        batch_stat = BatchStatement()
        for row in batch:
            batch_stat.add(prep_statement, row)
            #session.execute(basic_insert, row)
            #session.execute(prep_statement, row)
        session.execute(batch_stat)

    print "done, took: ", time.time() - t1
