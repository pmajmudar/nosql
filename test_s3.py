#!/usr/env/bin python

import gevent
from gevent import monkey
monkey.patch_all()

import boto
from boto.s3.key import Key
import threading
import uuid
import time

# Connect to S3
#s3 = boto.connect_s3()

# Fetch bucket
#bucket = s3.get_bucket('gi-app')


def set_data_and_print():
    "Set some basic data and printed."

    k = Key(bucket)
    k.key = 'prash1'
    k.set_contents_from_string('this is a test')
    results = bucket.list()
    for r in results:
        print r

def upload(insert_key):
    #s3 = boto.connect_s3()
    #bucket = s3.get_bucket('gi-perf-test-2')
    key = bucket.new_key(insert_key).set_contents_from_string('This is a test')
    return insert_key

def thread_insert():
    threads = []
    for key in test_keys:
        t = threading.Thread(target=upload, args=(key,))
        t.start()
        threads.append(t)

    for thread in threads:
        thread.join()



t0 = time.time()
s3 = boto.connect_s3()
bucket = s3.get_bucket('gi-perf-test-3')
print "time to connect + bucket ", time.time()-t0

test_keys = [uuid.uuid4().hex for i in range(20000)]
print "Starting"
t1 = time.time()
gpool = gevent.pool.Pool(size=100)
jobs = [gpool.spawn(upload, key) for key in test_keys]

gevent.joinall(jobs)
print "Done. ", time.time() - t1

results = bucket.list()
results = [result for result in results]
print len(results)
