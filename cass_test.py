from cassandra.cluster import Cluster

# by default connects to localhost
cluster = Cluster()

# create a session - connect to a keyspace
session = cluster.connect('prash')

rows = session.execute('SELECT * from test')


