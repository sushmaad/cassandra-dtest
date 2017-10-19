import time

from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement
from nose.tools import assert_equal, assert_true

import assertions
from dtest import create_cf


def create_c1c2_table(session, cf_name='cf', read_repair=None, compact_storage=True):
    create_cf(session, cf_name, columns={'c1': 'text', 'c2': 'text'}, read_repair=read_repair, compact_storage=compact_storage)


def insert_c1c2(session, keys=None,cf_name='cf', n=None, consistency=ConsistencyLevel.QUORUM):
    if (keys is None and n is None) or (keys is not None and n is not None):
        raise ValueError("Expected exactly one of 'keys' or 'n' arguments to not be None; "
                         "got keys={keys}, n={n}".format(keys=keys, n=n))
    if n:
        keys = list(range(n))

    statement = session.prepare("INSERT INTO {} (key, c1, c2) VALUES (?, 'value1', 'value2')".format(cf_name))
    statement.consistency_level = consistency

    execute_concurrent_with_args(session, statement, [['k{}'.format(k)] for k in keys])

def insert_cf(session, keys=None, n=None, consistency=ConsistencyLevel.QUORUM):
    if (keys is None and n is None) or (keys is not None and n is not None):
        raise ValueError("Expected exactly one of 'keys' or 'n' arguments to not be None; "
                         "got keys={keys}, n={n}".format(keys=keys, n=n))
    if n:
        keys = list(range(n))

    statement = session.prepare("INSERT INTO cf (key, c, v) VALUES (?, 'value1', 'value2')")
    statement.consistency_level = consistency

    execute_concurrent_with_args(session, statement, [['k{}'.format(k)] for k in keys])


def query_c1c2(session, key, cf_name='cf', consistency=ConsistencyLevel.QUORUM, tolerate_missing=False, must_be_missing=False):
    query = SimpleStatement('SELECT c1, c2 FROM  %s WHERE key=\'k%d\'' % (cf_name, key), consistency_level=consistency)
    rows = list(session.execute(query))
    if not tolerate_missing:
        assertions.assert_length_equal(rows, 1)
        res = rows[0]
        assert_true(len(res) == 2 and res[0] == 'value1' and res[1] == 'value2', res)
    if must_be_missing:
        assertions.assert_length_equal(rows, 0)


def insert_columns(tester, session, key, columns_count, consistency=ConsistencyLevel.QUORUM, offset=0):
    upds = ["UPDATE cf SET v=\'value%d\' WHERE key=\'k%s\' AND c=\'c%06d\'" % (i, key, i) for i in xrange(offset * columns_count, columns_count * (offset + 1))]
    query = 'BEGIN BATCH %s; APPLY BATCH' % '; '.join(upds)
    simple_query = SimpleStatement(query, consistency_level=consistency)
    session.execute(simple_query)


def query_columns(tester, session, key, columns_count, consistency=ConsistencyLevel.QUORUM, offset=0):
    query = SimpleStatement('SELECT c, v FROM cf WHERE key=\'k%s\' AND c >= \'c%06d\' AND c <= \'c%06d\'' % (key, offset, columns_count + offset - 1), consistency_level=consistency)
    res = list(session.execute(query))
    assertions.assert_length_equal(res, columns_count)
    for i in xrange(0, columns_count):
        assert_equal(res[i][1], 'value{}'.format(i + offset))


# Simple puts and get (on one row), testing both reads by names and by slice,
# with overwrites and flushes between inserts to make sure we hit multiple
# sstables on reads
def putget(cluster, session, cl=ConsistencyLevel.QUORUM):

    _put_with_overwrite(cluster, session, 1, cl)

    # reads by name
    # We do not support proper IN queries yet
    # if cluster.version() >= "1.2":
    #    session.execute('SELECT * FROM cf USING CONSISTENCY %s WHERE key=\'k0\' AND c IN (%s)' % (cl, ','.join(ks)))
    # else:
    #    session.execute('SELECT %s FROM cf USING CONSISTENCY %s WHERE key=\'k0\'' % (','.join(ks), cl))
    # _validate_row(cluster, session)
    # slice reads
    query = SimpleStatement('SELECT * FROM cf WHERE key=\'k0\'', consistency_level=cl)
    rows = list(session.execute(query))
    _validate_row(cluster, rows)


def _put_with_overwrite(cluster, session, nb_keys, cl=ConsistencyLevel.QUORUM):
    for k in xrange(0, nb_keys):
        kvs = ["UPDATE cf SET v=\'value%d\' WHERE key=\'k%s\' AND c=\'c%02d\'" % (i, k, i) for i in xrange(0, 100)]
        query = SimpleStatement('BEGIN BATCH %s APPLY BATCH' % '; '.join(kvs), consistency_level=cl)
        session.execute(query)
        time.sleep(.01)
    cluster.flush()
    for k in xrange(0, nb_keys):
        kvs = ["UPDATE cf SET v=\'value%d\' WHERE key=\'k%s\' AND c=\'c%02d\'" % (i * 4, k, i * 2) for i in xrange(0, 50)]
        query = SimpleStatement('BEGIN BATCH %s APPLY BATCH' % '; '.join(kvs), consistency_level=cl)
        session.execute(query)
        time.sleep(.01)
    cluster.flush()
    for k in xrange(0, nb_keys):
        kvs = ["UPDATE cf SET v=\'value%d\' WHERE key=\'k%s\' AND c=\'c%02d\'" % (i * 20, k, i * 5) for i in xrange(0, 20)]
        query = SimpleStatement('BEGIN BATCH %s APPLY BATCH' % '; '.join(kvs), consistency_level=cl)
        session.execute(query)
        time.sleep(.01)
    cluster.flush()


def _validate_row(cluster, res):
    assertions.assert_length_equal(res, 100)
    for i in xrange(0, 100):
        if i % 5 == 0:
            assert_equal(res[i][2], 'value{}'.format(i * 4), 'for {}, expecting value{}, got {}'.format(i, i * 4, res[i][2]))
        elif i % 2 == 0:
            assert_equal(res[i][2], 'value{}'.format(i * 2), 'for {}, expecting value{}, got {}'.format(i, i * 2, res[i][2]))
        else:
            assert_equal(res[i][2], 'value{}'.format(i), 'for {}, expecting value{}, got {}'.format(i, i, res[i][2]))


# Simple puts and range gets, with overwrites and flushes between inserts to
# make sure we hit multiple sstables on reads
def range_putget(cluster, session, cl=ConsistencyLevel.QUORUM):
    keys = 100

    _put_with_overwrite(cluster, session, keys, cl)

    paged_results = session.execute('SELECT * FROM cf LIMIT 10000000')
    rows = [result for result in paged_results]

    assertions.assert_length_equal(rows, keys * 100)
    for k in xrange(0, keys):
        res = rows[:100]
        del rows[:100]
        _validate_row(cluster, res)


def get_keyspace_metadata(session, keyspace_name):
    cluster = session.cluster
    cluster.refresh_keyspace_metadata(keyspace_name)
    return cluster.metadata.keyspaces[keyspace_name]


def get_schema_metadata(session):
    cluster = session.cluster
    cluster.refresh_schema_metadata()
    return cluster.metadata


def get_table_metadata(session, keyspace_name, table_name):
    cluster = session.cluster
    cluster.refresh_table_metadata(keyspace_name, table_name)
    return cluster.metadata.keyspaces[keyspace_name].tables[table_name]


def rows_to_list(rows):
    new_list = [list(row) for row in rows]
    return new_list


def index_is_built(node, session, keyspace, table_name, idx_name):
    # checks if an index has been built
    full_idx_name = idx_name if node.get_cassandra_version() > '3.0' else '{}.{}'.format(table_name, idx_name)
    index_query = """SELECT * FROM system."IndexInfo" WHERE table_name = '{}' AND index_name = '{}'""".format(keyspace, full_idx_name)
    return len(list(session.execute(index_query))) == 1
