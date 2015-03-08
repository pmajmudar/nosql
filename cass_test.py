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
# NB can also use session.set_keyspace('') OR session.execute('USE prash')
session = cluster.connect('prash')

# Create an example table
# Can alter table as follows: ALTER TABLE test ADD silver_pop text;
session.execute("""CREATE TABLE IF NOT EXISTS prash.test2(
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

rows = session.execute('SELECT * from test limit 5')
print rows

prep_statement = session.prepare("""INSERT INTO prash.test2 (url, final_url, doc_id, run_id, doubleclick_fl, new_relic, comscore, facebook_li, crazy_egg, omniture, mediamath) VALUES (?,?,?,?,?,?,?,?,?,?,?)""")


basic_insert = """INSERT INTO prash.test2 (url, final_url, doc_id, run_id, doubleclick_fl, new_relic, comscore, facebook_li, crazy_egg, omniture, mediamath) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

def read_csv_data():
    """Read test dataset."""

    import csv

    print "reading data..."
    import time
    t1 = time.time()
    reader = csv.reader(open('/var/data/crawl_parsers/tmp/feb2015_customeranalytics_10595.csv', 'r'))
    header = reader.next()
    rows = [row for row in reader]
    time_to_read = time.time() - t1

    print "number of rows = ", len(rows), " took: ", time_to_read
    return rows

def make_batch(rows, batchsize=1):
    row_len = len(rows)
    for idx in range(0, row_len, batchsize):
        yield rows[idx:min(idx+batchsize, row_len)]


def test_batch_insert(rows):
    """Test insertion using batch / prepared statements."""

    print "inserting data"
    t1 = time.time()
    batch_gen = make_batch(rows, batchsize=5000)
    for batch in batch_gen:
        batch_stat = BatchStatement()
        for row in batch:
            batch_stat.add(prep_statement, row)

        session.execute(batch_stat)

    print "done, took: ", time.time() - t1

def test_async_insert(rows):
    """Test insertion async."""

    from itertools import count
    from threading import Event
    print "Inserting data..."
    t1 = time.time()
    finished = Event()
    def insert_next(result_or_failure=None):
        if isinstance(result_or_failure, BaseException):
            print "Error"

        if rows:
            row = rows.pop()
            future = session.execute_async(prep_statement, row)
            future.add_callbacks(insert_next, insert_next)
        else:
            finished.set()

    for i in range(10):
        insert_next()
    finished.wait()
    print "Done, took: ", time.time() - t1


if __name__ == "__main__":
    #test_batch_insert(rows)
    #test_async_insert(rows)
    test_read()
