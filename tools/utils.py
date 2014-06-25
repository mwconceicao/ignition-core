#!/usr/bin/env python
import logging
import boto

logging.basicConfig(level=logging.INFO)

def get_active_instances(conn):
    active = [instance for res in conn.get_all_instances()
              for instance in res.instances
              if instance.state in set(['pending', 'running',
                                        'stopping', 'stopped'])]
    return active

def parse_nodes(active_instances, cluster_name):
    master_nodes = []
    slave_nodes = []
    for instance in active_instances:
        group_names = [g.name for g in instance.groups]
        if (cluster_name + "-master") in group_names:
            master_nodes.append(instance)
        elif (cluster_name + "-slaves") in group_names:
            slave_nodes.append(instance)
    return (master_nodes, slave_nodes)

def get_masters(cluster_name):
    conn = boto.connect_ec2()
    active = get_active_instances(conn)
    master_nodes, slave_nodes = parse_nodes(active, cluster_name)
    return master_nodes

def tag_instances(cluster_name, tags):
    conn = boto.connect_ec2()

    active = get_active_instances(conn)
    logging.info('%d active instances', len(active))

    master_nodes, slave_nodes = parse_nodes(active, cluster_name)
    logging.info('%d master, %d slave', len(master_nodes), len(slave_nodes))

    if master_nodes:
        conn.create_tags([i.id for i in master_nodes],
                         {'spark_node_type': 'master'})
    if slave_nodes:
        conn.create_tags([i.id for i in slave_nodes],
                         {'spark_node_type': 'slave'})

    if slave_nodes or master_nodes:
        ids = [i.id for l in (master_nodes, slave_nodes) for i in l]
        conn.create_tags(ids, tags)

    logging.info("Tagged nodes.")
