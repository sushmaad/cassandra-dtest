from cassandra import ConsistencyLevel

import time

from dtest import DISABLE_VNODES, Tester, create_ks, create_ccm_cluster, create_cf, debug, cleanup_cluster, \
    get_test_path
from tools.data import create_c1c2_table, insert_c1c2, query_c1c2
from tools.decorators import no_vnodes, since

class TestHintedService(Tester):

    #def __delete__(self):
    #    print ("In TestHintedService destructor")
    #    self.__cleanup_hints_cluster()

    def __cleanup_hints_cluster(self):
        # copy logs
        #self.copy_logs(self._hints_cluster)
        cleanup_cluster(self._hints_cluster, self._hints_test_path)

    def __nodetool_cmd(self, node, cmd):
        """
        Launch a nodetool command and check there is no error, return the result
        """
        out, err, _ = node.nodetool(cmd)
        #self.assertEqual('', err, msg=err)
        return out

    def __start_hints_cluster(self, name='hints_test_hints'):
        """
        Start a hints cluster with 2 nodes and  1 region
        """
        self._hints_test_path = get_test_path()
        self._hints_cluster = create_ccm_cluster(self._hints_test_path, name)
        self._hints_rpc_addr = ""
        self._hints_rpc_port = 0
        self._hints_cluster.set_configuration_options(values={'num_tokens': '1'})
        self._hints_cluster.populate([2]).start(wait_for_binary_proto=True)

        node = self._hints_cluster.nodelist()[0]
        out = self.__nodetool_cmd(node, "enablethrift")
        out = self.__nodetool_cmd(node, "statusthrift")
        debug("enabled thrift for hint cluster {}" .format(out))
        self._hints_rpc_addr = self.__nodetool_cmd(node, "getrpcaddress")
        self._hints_rpc_port = self.__nodetool_cmd(node, "getrpcport")
        print ("rpc {} : {}".format(self._hints_rpc_addr , self._hints_rpc_port))

    def __start_cluster(self):
        """
        Start a cluster with 2 nodes and return them
        in order to start the cluster successfully make sure
        loopback alias is setup for 127.0.0.21 and 127.0.0.22 since ipprefix provided
        for cluster.populate is 127.0.0.2
        """
        cluster = self.cluster = create_ccm_cluster(self.test_path, name='hints_test_main')

        # Both nodes do not join the ring immediately
        cluster.set_configuration_options(values={'hinted_handoff_enabled': 'true', 'num_tokens': 1})

        cluster.populate([2], debug=True, ipprefix='127.0.0.2', jmx_port=7010).start(
            join_ring=False,
            wait_for_binary_proto=True
        )
        cluster.nodelist()[0].show()
        cluster.nodelist()[1].show()

        return cluster.nodelist()


    def simple_hintsreadwrite_test(self):
        """
        Test single region setup, with two data node
        1. data node fail before write
        2. send phd write request
        2. bring up the data node
        3. read from the region
        4. verify reads, should get consistent data
        """
        name = 'hints_test_hints'
        self.__start_hints_cluster(name=name)
        try:
            [coordinator, datanode] = self.__start_cluster()

            # data node joins the ring and sets the hintsservice
            self.__nodetool_cmd(datanode, "join")
            ring_status = self.__nodetool_cmd(datanode, "ring")
            print ("Ring status of cluster {}".format(ring_status))
            hintsservicecmd = "sethintsservice thrift --hintsaddr {} --hintsport {}".format(self._hints_rpc_addr, self._hints_rpc_port)
            self.__nodetool_cmd(datanode, hintsservicecmd)

            debugcmd = "setlogginglevel"
            self.__nodetool_cmd(coordinator, debugcmd)
            self.__nodetool_cmd(datanode, debugcmd)

            time.sleep(10)
            keyspace = "test_hints"
            cf_name = "t0"
            session = self.patient_cql_connection(coordinator)
            create_ks(session, keyspace, 1)
            create_c1c2_table(session, cf_name, compact_storage=True)

            time.sleep(10)
            hint_node1 = self._hints_cluster.nodelist()[0]
            session1 = self.patient_cql_connection(hint_node1)
            create_ks(session1, keyspace, 1)
            create_c1c2_table(session1, cf_name, compact_storage=True)
            time.sleep(10)

            # insert and validate
            insert_c1c2(session, keys=[1, 2, 3], cf_name=cf_name, consistency=ConsistencyLevel.ONE)
            query_c1c2(session, 1, cf_name=cf_name, consistency=ConsistencyLevel.ONE)

            time.sleep(10)

            datanode.stop(wait_other_notice=True)

            time.sleep(10)

            insert_c1c2(session, keys=[4, 5, 6], cf_name=cf_name, consistency=ConsistencyLevel.PHD)
            datanode.start(wait_for_binary_proto=True)
            time.sleep(10)
            query_c1c2(session, 4, cf_name=cf_name, consistency=ConsistencyLevel.PHD)

        finally:
            self.__cleanup_hints_cluster()

        #time.sleep(300)

