#!/usr/bin/env python
import os, sys, collections, itertools, logging, traceback, subprocess, datetime, base64, pickle, time, uuid, json
import boto.ec2
#import librato, graypy
import requests
import math
script_path = os.path.dirname(os.path.realpath(__file__))
user_data_script = os.path.join(script_path, 'user_data_ansible_pull.sh')
sys.path.insert(0, os.path.join(script_path, '..', 'core', 'tools'))
import cluster

## Pseudo-types definition
ClusterConf = collections.namedtuple('ClusterConf', ['instance_type', 'hourly_budget', 'preference_rate', 'slaves', 'worker_instances', 'job_mem', 'ami_type'])
RegionConf = collections.namedtuple('RegionConf', ['region', 'ami_pvm', 'ami_hvm', 'az_suffixes', 'vpc', 'subnet_by_az'])
FullConf = collections.namedtuple('FullConf', ['cluster_conf', 'region_conf', 'az'])

## Constants
default_security_group = 'Ignition'
env = 'prod'
sanity_check_job_name = 'HelloWorldSetup'
sanity_check_job_timeout_minutes = 7
spark_version = '1.3.0'
master_ami_type = 'pvm'

job_timeout_minutes = 240

regions_conf = collections.OrderedDict([
    ('us-east-1', RegionConf('us-east-1', 'ami-5bb18832', 'ami-35b1885c', ['b', 'c', 'd', 'e'], 'vpc-d92a61bc',
        collections.OrderedDict([
            ('us-east-1b', 'subnet-d3a511f8'),
            ('us-east-1c', 'subnet-cbcca3bc'),
            ('us-east-1d', 'subnet-120b8e4b'),
            ('us-east-1e', 'subnet-fada92c0')])
        )
    ),
#    ('us-west-2', RegionConf('us-west-2', 'ami-9a6e0daa', 'ami-ae6e0d9e', ['a', 'b', 'c'])),
#    ('us-west-1', RegionConf('us-west-1', 'ami-7a320f3f', 'ami-72320f37', ['a', 'b', 'c'])),
])

master_instance_type = 'm3.2xlarge'
runner_eid = "runner-{0}".format(uuid.uuid4())
victorops_url = "https://alert.victorops.com/integrations/generic/20131114/alert/TBD"


# Used for contextual logging
namespace = "generation"
current_job = "generation"

## LOGGING
class ContextFilter(logging.Filter):
    def filter(self, record):
        record.namespace = namespace
        record.setupName = current_job # use same key name used in ignition
        record.application = 'job-runner'
        return True

log = logging.getLogger()
log.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
#log.addHandler(graypy.GELFHandler('TBD-graylog host'))
for handler in log.handlers:
    handler.setFormatter(formatter)
log.addFilter(ContextFilter())

# This is not a cluster sequence anymore. Now it's used to generate a cluster sequence.
# Job Runner look for the best cost-benefit cluster according to following steps:
#
# 1 - List the prices of these configurations for all available AZs.
# 2 - Apply the preference_rate for each instance price generating a "cost-benefit" index.
# 3 - Pick the cluster with best cost-benefit
#
# Example:
#
# cluster_budget  = 3.0
# cluster_options = [
#     ClusterConf('r3.8xlarge', cluster_budget, 1, '8', '4', '30G', 'hvm'),
#     ClusterConf('c3.4xlarge', cluster_budget, 0.7, '30', '1', '20G', 'hvm'),
# ]
#
# According to configuration above, job_runner will iterate over the following table until it finds a good cluster configuration.
#
# instance_type   az  price*  slaves  hourly_cost rate    weighted_cost**
# c3.4xlarge      b   0.0450  30      1.3950      0.7     1.99             <-- It's not my preferred cluster, but it is ~30% cheaper than the 2o one, so I'll buy it.
# r3.8xlarge      b   0.2500  8       2.0000      1.0     2.00             <-- Our second option. Will try this if previous choice fail.
# c3.4xlarge      e   0.0500  30      1.5000      0.7     2.14
# r3.8xlarge      a   0.2900  8       2.3200      1.0     2.32
# r3.8xlarge      e   0.3300  8       2.6400      1.0     2.64             <-- Panic alert.
# c3.4xlarge      a   0.1300  30      3.9000      0.7     5.57             <--- Expensive and/or relatively bad clusters. job_runner will skip them.
# c3.4xlarge      c   0.1600  30      4.8000      0.7     6.86             <--|
# c3.4xlarge      d   0.1700  30      5.1000      0.7     7.29             <--|
# r3.8xlarge      c   6.0000  8       48.0000     1.0     48.00            <--/
#
#
# Ancient job_runner.py would work this way:
# 1 - List the prices of the first configuration for all available AZs.
# 2 - Pick the cluster with best cost among those available AZs.
#
# cluster_sequence = [
#     ClusterConf('r3.8xlarge', 0.375, '8', '4', '30G', 'hvm'),
#     ClusterConf('c3.4xlarge', 0.100, '30', '1', '20G', 'hvm'),
# ]
#
# According to configuration above, job_runner would iterate over configurations trying to find the cheaper AZ for the given instance.
#
# instance_type   az  price*  slaves  hourly_cost
# r3.8xlarge      b   0.2500  8       2.0000        <-- This AZ is cheapest enough, so I will buy it (without check c3.4xlarge instances).
# r3.8xlarge      a   0.2900  8       2.3200
# r3.8xlarge      e   0.3300  8       2.6400
# r3.8xlarge      c   6.0000  8       48.0000       <-- Wouldn't consider. Too expensive.
# c3.4xlarge      b   0.0450  30      1.3950        <--- Ignored for now, because r3.8xlarge - b is cheap enough. It would fallback to this only when previous choice fail.
# c3.4xlarge      e   0.0500  30      1.5000        <--|
# c3.4xlarge      a   0.1300  30      3.9000        <--|
# c3.4xlarge      c   0.1600  30      4.8000        <--|
# c3.4xlarge      d   0.1700  30      5.1000        <--/
#
# So...
# Previous job_runner.py would pick a r3.8xlarge with 8 slaves in us-east-1a costing 2.00/hour.
# Current job_runner.py will pick a c3.4xlarge with 30 slaves in us-east-1b costing 1.39/hour.
# Bid for entire cluster would be the same in both cases: 3.00/hour.
#
# * This is not the current spot price, but max(historical_price, recent_price)
# ** Weighted cost is used only for sorting purpose.

## Cluster Configuration

search_generation_cluster_budget = 0.26 * 2 * 3 * 1.2 * 2
search_generation_cluster_options = [
    ClusterConf('r3.8xlarge', search_generation_cluster_budget, 1, '1', '8', '15G', 'hvm'),
    ClusterConf('r3.4xlarge', search_generation_cluster_budget, 1, '1', '4', '15G', 'hvm'),
    ClusterConf('r3.2xlarge', search_generation_cluster_budget, 1, '2', '2', '15G', 'hvm'),
    ClusterConf('r3.xlarge', search_generation_cluster_budget, 1, '4', '1', '15G', 'hvm'),

    ClusterConf('d2.8xlarge', search_generation_cluster_budget, 0.8, '1', '8', '15G', 'hvm'),
    ClusterConf('d2.4xlarge', search_generation_cluster_budget, 0.8, '1', '4', '15G', 'hvm'),
    ClusterConf('d2.2xlarge', search_generation_cluster_budget, 0.8, '2', '2', '15G', 'hvm'),
    ClusterConf('d2.xlarge', search_generation_cluster_budget, 0.8, '4', '1', '15G', 'hvm'),

    ClusterConf('c3.8xlarge', search_generation_cluster_budget, 0.7, '1', '3', '15G', 'hvm'),
    ClusterConf('c3.4xlarge', search_generation_cluster_budget, 0.7, '2', '1', '15G', 'hvm'),
    ClusterConf('c3.2xlarge', search_generation_cluster_budget, 0.7, '4', '1', '7G', 'hvm'),

    ClusterConf('hi1.4xlarge', search_generation_cluster_budget, 0.6, '4', '2', '15G', 'hvm'),

    ClusterConf('cc2.8xlarge', search_generation_cluster_budget, 0.6, '2', '2', '15G', 'hvm'),
]


def send_heartbeat(retries=3):
    for i in range(retries):
        try:
            user = 'user'
            token = 'token'
            #FIXME: disabled
            #api = librato.connect(user, token)
            #api.submit('runner.{0}.heartbeat'.format(namespace), 1)
            break
        except Exception as e:
            log.exception('Exception sending health check to librato')
            notify("Exception sending health check to librato\n\nException is: " + traceback.format_exc(), severity="WARNING")
            time.sleep(1)

def victorops_alert(entity_id, severity="CRITICAL", message=""):
    message = {
        "message_type": severity,
        "entity_id": entity_id,
        "timestamp": int(time.time()),
        "state_message": message,
    }
    #FIXME: disabled
    #r = requests.post(victorops_url, data=json.dumps(message))
    #return r.text

def notify(message, severity="CRITICAL", entity_id=runner_eid):
    try:
        return victorops_alert(entity_id, severity, message)
    except Exception as e:
        log.exception('This is bad, notification failed. Someone must be notified about this.')


def avg(it):
    l = list(it)
    return sum(l) / len(l) if len(l) > 0 else 0


def get_avg_worst_price(hprices, n):
    return avg(sorted([h.price for h in hprices], reverse=True)[:n])


def get_date_start_at(hours_past, now=None):
    """
    >>> get_date_start_at(7, datetime.datetime(2014, 5, 1, 0, 0, 0))
    '2014-04-30T17:00:00.000Z'
    """
    now = now or datetime.datetime.utcnow()
    return (now - datetime.timedelta(hours=hours_past)).isoformat()[:19] + '.000Z'


def get_prices_after_hours_ago(hprices, hours):
    timestamp = get_date_start_at(hours)
    return [h for h in hprices if h.timestamp >= timestamp]


def get_azs_for_region(region_conf):
    return [region_conf.region + a for a in region_conf.az_suffixes]


def get_cluster_conf_prices(cluster_conf, product_description):
    for region, region_conf in regions_conf.items():
        conn = boto.ec2.connect_to_region(region)
        for az in get_azs_for_region(region_conf):
            full_conf = FullConf(cluster_conf, region_conf, az)
            try:
                prices = conn.get_spot_price_history(instance_type=cluster_conf.instance_type, availability_zone=az, product_description=product_description)
            except Exception as e:
                log.exception("Unable to get spot instance price.")
                continue

            # Get the avg of the 10 worst prices in last 2 days
            average_worst_historical_price = get_avg_worst_price(get_prices_after_hours_ago(prices, 48), 10)

            # Get the prices from approx. the last 6 hours and pick the worst
            average_worst_recent_price = get_avg_worst_price(get_prices_after_hours_ago(prices, 6), 1)

            if average_worst_recent_price > 0 and average_worst_historical_price > 0:
                instance_price = max(average_worst_recent_price, average_worst_historical_price)
                yield instance_price, full_conf


def get_cluster_sequence(cluster_options, product_description):
    log.info('Getting spot price history... This can take a while.')
    def weighted_cluster_confs():
        for cluster_conf in cluster_options:
            if cluster_conf.hourly_budget is not None:
                for instance_price, full_conf in get_cluster_conf_prices(cluster_conf, product_description):
                    cluster_conf = full_conf.cluster_conf
                    cluster_cost = int(cluster_conf.slaves) * instance_price
                    preference_factor = max(cluster_conf.preference_rate, 1e-9)
                    weighted_cluster_cost = cluster_cost / preference_factor
                    yield weighted_cluster_cost, instance_price, cluster_cost, full_conf
            else:
                for region, region_conf in regions_conf.items():
                    for az in get_azs_for_region(region_conf):
                        yield 1/1e-12, None, None, FullConf(cluster_conf, region_conf, az)

    for weight, instance_price, cluster_cost, full_conf in sorted(list(weighted_cluster_confs())):
        yield cluster_cost, instance_price, full_conf

def select_cheapest_confs(cluster_sequence):
    for cluster_cost, instance_price, full_conf in cluster_sequence:
        cluster_conf = full_conf.cluster_conf
        instance = cluster_conf.instance_type
        slaves = cluster_conf.slaves
        az = full_conf.az
        budget = cluster_conf.hourly_budget

        if cluster_conf.hourly_budget is None:
            # TODO: Make ondemand instances be set as parameter.
            log.info('A ondemand cluster {0} with {1} slaves will potentially cost us pools of money! Be smart!'.format(instance, slaves))
            yield full_conf
        elif cluster_conf.hourly_budget >= cluster_cost:
            log.info('A {0}({4}/slave) cluster with {1} slaves will cost {2}/hour in {3}. It fits in our budget of {5}/hour'.format(instance, slaves, cluster_cost, az, instance_price, budget))
            yield full_conf
        else:
            log.info('A {0}({4}/slave) cluster with {1} slaves will cost {2}/hour in {3}. Too expensive! We can spend only {5}/hour'.format(instance, slaves, cluster_cost, az, instance_price, budget))


def get_best_conf_and_az(blacklisted_confs, cluster_options, entity_id=runner_eid, disable_vpc=False):
    def inotify(message, severity="CRITICAL"):
        return notify(message, severity, entity_id=entity_id)

    def report_info(message, severity="INFO"):
        log.info(message)
        return notify(message, severity, entity_id=entity_id)

    product_description = 'Linux/UNIX' if disable_vpc else 'Linux/UNIX (Amazon VPC)'

    while True:
        send_heartbeat()
        try:
            cluster_sequence = list(get_cluster_sequence(cluster_options, product_description))
            cheap_sequence = list(select_cheapest_confs(cluster_sequence))

            if len(blacklisted_confs) >= len(cluster_sequence):
                log.warn('We have blacklisted all confs, cleaning blacklist')
                notify('All cluster configurations have been blacklisted', 'All cluster configurations have been blacklisted, as an emergencial action we are cleaning this blacklist. This issue must be investigated.')
                blacklisted_confs = ExpireCollection()

            for full_conf in cheap_sequence:
                cluster_conf = full_conf.cluster_conf

                if full_conf in blacklisted_confs:
                    report_info("Ignoring this conf because it's blacklisted for now: {0}".format(full_conf))
                    continue

                if cluster_conf.preference_rate <= 0:
                    report_info("This cluster have a good price, but it's not interesting. You should consider remove this conf: {0}".format(cluster_conf))
                    continue

                if cluster_conf.hourly_budget is not None:
                    log.info('Working with {0} spot instances of {1} in {2}'.format(cluster_conf.slaves, cluster_conf.instance_type, full_conf.az))

                    if full_conf != cheap_sequence[0]:
                        if full_conf == cheap_sequence[-1]:
                            inotify('Urgent: Imminent cluster failure' "\n" 'We are using our last cluster option. The processing is close to a halt if this cluster fails. By the way, this cluster is expensive and potentially slow.')
                        elif cheap_sequence.index(full_conf) >= len(cheap_sequence) // 2:
                            inotify('Spot problems __right now__' "\n" 'We are using one of our last cluster configurations. There is a risk we may face a halt in the processing if the problem persists and no action is taken.')
                        else:
                            inotify('Spot problems ahead' "\n" 'We are not running on our first cluster choice. This indicates the market is bad and our performance may be reduced.', severity="WARNING")

                    return full_conf
                else:
                    report_info('Working with {0} ondemand instances of {1} in {2}'.format(cluster_conf.slaves, cluster_conf.instance_type, full_conf.az))
                    return full_conf

            log.info('No cluster configuration is economically viable!')
            inotify('Extremely urgent: All processing is halted!' "\n" 'Time to panic. No cluster configuration is economically viable. Probably the spot market crashed for good. Immediate action is necessary to restore the cluster.')
        except Exception as e:
            log.exception('Exception while getting spot price history')
            inotify('Exception while getting spot price history' "\n" 'This is very weird. If it happens more than once, better panic. Exception is: ' + traceback.format_exc(), severity="WARNING")
        time.sleep(60)

class AssemblyFailedException(Exception): pass

def build_assembly():
    send_heartbeat()
    try:
        cluster.build_assembly()
    except Exception as e:
        log.exception('Failed to build assembly')
        raise AssemblyFailedException()

def killall_jobs(cluster_name, region):
    send_heartbeat()
    try:
        cluster.killall_jobs(cluster_name, region=region)
    except Exception as e:
        pass

def run_sanity_checks(collect_results_dir, full_conf, cluster_name):
    send_heartbeat()
    log.info('Running sanity check')
    region = full_conf.region_conf.region
    cluster_conf = full_conf.cluster_conf
    cluster.health_check(cluster_name=cluster_name, region=region)
    cluster_job_run(cluster_name=cluster_name, job_name=sanity_check_job_name, job_mem=cluster_conf.job_mem,
                    region=region, job_timeout_minutes=sanity_check_job_timeout_minutes,
                    collect_results_dir=collect_results_dir,
                    detached=True, kill_on_failure=True, disable_assembly_build=True)

def destroy_all_clusters(cluster_name):
    send_heartbeat()
    for region in regions_conf:
        cluster.destroy(cluster_name, region=region)

def cluster_job_run(*args, **kwargs):
    send_heartbeat()
    cluster.job_run(*args, **kwargs)

def cluster_destroy(*args, **kwargs):
    send_heartbeat()
    cluster.destroy(*args, **kwargs)

def cluster_launch(*args, **kwargs):
    send_heartbeat()
    cluster.launch(*args, **kwargs)

def save_conf_on_cluster(full_conf, cluster_name):
    send_heartbeat()
    cluster.save_extra_data(object_to_base64(full_conf), cluster_name, region=full_conf.region_conf.region)

def load_conf_from_cluster(cluster_name):
    send_heartbeat()
    for region in regions_conf:
        try:
            data = cluster.load_extra_data(cluster_name, region=region)
            conf = base64_to_object(data)
            log.info('Found conf {0} from existing cluster'.format(conf))
            return conf
        except Exception as e:
            pass
    log.info('No existing cluster conf found')
    return None

def get_ami_for(full_conf):
    if full_conf.cluster_conf.ami_type == 'hvm':
        return full_conf.region_conf.ami_hvm
    else:
        return full_conf.region_conf.ami_pvm

def get_master_ami(full_conf):
    if master_ami_type == 'hvm':
        return full_conf.region_conf.ami_hvm
    else:
        return full_conf.region_conf.ami_pvm

def ensure_cluster(cluster_name, cluster_options, full_conf, blacklisted_confs, collect_results_dir, entity_id, disable_vpc, spark_version=spark_version, security_group=default_security_group, tag=[]):
    while True:
        try:
            full_conf = load_conf_from_cluster(cluster_name)
            if not full_conf:
                log.info('No existing cluster found, destroying all possible half-created clusters then proceeding to create a new cluster')
                destroy_all_clusters(cluster_name)
                full_conf = get_best_conf_and_az(blacklisted_confs, cluster_options, entity_id=entity_id, disable_vpc=disable_vpc)

                if full_conf.cluster_conf.hourly_budget is not None:
                    ondemand = False
                    precise_spot_price = float(full_conf.cluster_conf.hourly_budget) / int(full_conf.cluster_conf.slaves)
                    spot_price = str(math.ceil(precise_spot_price * 100) / 100.0)
                else:
                    ondemand = True
                    spot_price = None

                log.info('Trying to launch cluster for {0} with configuration {1}'.format(spot_price, full_conf))
                cluster_launch(cluster_name=cluster_name, slaves=str(full_conf.cluster_conf.slaves),
                               master_instance_type=master_instance_type,
                               instance_type=full_conf.cluster_conf.instance_type,
                               ondemand=ondemand,
                               spot_price=spot_price,
                               ami=get_ami_for(full_conf),
                               master_ami=get_master_ami(full_conf),
                               worker_instances=str(full_conf.cluster_conf.worker_instances),
                               zone=full_conf.az,
                               vpc = full_conf.region_conf.vpc if not disable_vpc else None,
                               vpc_subnet = full_conf.region_conf.subnet_by_az.get(full_conf.az) if not disable_vpc else None,
                               just_ignore_existing=False,
                               spark_version=spark_version,
                               security_group=security_group, env=env,
                               region=full_conf.region_conf.region, max_clusters_to_create=1,
                               user_data=user_data_script, tag=tag,
                               script_timeout_total_minutes=110,
                               script_timeout_inactivity_minutes=20)
                save_conf_on_cluster(full_conf, cluster_name)
            killall_jobs(cluster_name, full_conf.region_conf.region)
            try:
                run_sanity_checks(collect_results_dir, full_conf, cluster_name)
            except Exception as e:
                log.exception('Sanity check failed')
                notify('Sanity check failed' "\n" 'It will be checked once more before destroying the cluster', severity="WARNING", entity_id=entity_id)
            run_sanity_checks(collect_results_dir, full_conf, cluster_name)
            break
        except Exception as e:
            if full_conf:
                blacklisted_confs.add(full_conf)
            log.exception('Cluster failed')
            notify("""Cluster failed

But don't panic. We will try creating another cluster. See log for more details.
Our current cluster configuration is {0}
Exception is: {1}
"""
                    .format(full_conf, traceback.format_exc()), severity="WARNING", entity_id=entity_id)
            cluster_destroy(cluster_name, region=full_conf.region_conf.region)
    return full_conf


def run_job(cluster_name, job_name, full_conf, collect_results_dir, consecutive_failures=0, max_errors_on_healthy_cluster = 5, entity_id=runner_eid):
    while True:
        try:
            log.info('Running {}'.format(job_name))
            cluster_job_run(cluster_name=cluster_name, job_name=job_name, job_mem=full_conf.cluster_conf.job_mem,
                            region=full_conf.region_conf.region, job_timeout_minutes=(job_timeout_minutes),
                            collect_results_dir=collect_results_dir,
                            detached=True, kill_on_failure=True, disable_assembly_build=True)
            consecutive_failures = 0
            notify('Job execution completed successfully :)', severity="RESOLVE", entity_id=entity_id)
            return (True, 0)
        except Exception as e_job:
            consecutive_failures += 1
            log.exception('Job execution failed')
            notify("""Job execution failed

But don't panic. We will try again. See log for more details.
We had {0} consecutive failures of maximum of {1}.
Exception is: {2}
"""
                   .format(consecutive_failures, max_errors_on_healthy_cluster, traceback.format_exc()), severity="WARNING", entity_id=entity_id)
            try:
                run_sanity_checks(collect_results_dir, full_conf, cluster_name)
                if consecutive_failures >= max_errors_on_healthy_cluster:
                    log.error('Max number of consecutive failures reached. Killing healthy cluster =(')
                    notify("Killing healthy cluster =(" "\n\n"
                           "Max number of consecutive failures reached. Something is very very wrong. As always, we will try again, but better check those logs.",
                           severity="CRITICAL", entity_id=entity_id)
                    cluster_destroy(cluster_name, region=full_conf.region_conf.region)
                    return (False, 0)
            except Exception as e_sanity:
                log.exception('Sanity check failed')
                notify("Sanity check failed\nThe cluster was healthy but now it looks unhealthy. More checks will come soon.", severity="WARNING", entity_id=entity_id)
                return (False, consecutive_failures)


def setup(job_name):
    setup_eid = "{0}-{1}".format(job_name, runner_eid)
    notify("Initializing job runner\nIt's a pleasure to be back.", severity="INFO", entity_id=setup_eid)
    while True:
        try:
            if cluster.get_assembly_path() is None:
                build_assembly()
            break
        except AssemblyFailedException as e:
            notify('Failed to build assembly' + "\n" +
                   'Someone messed up with the installation/deploy. An urgent action is needed. Exception is: ' + traceback.format_exc(),
                   entity_id=setup_eid)


from argh import *


def run_continuously(collect_results_dir, namespace_, job_name, cluster_name_prefix, cluster_options,
                     disable_vpc=False, security_group=default_security_group, tag=[], max_errors_on_healthy_cluster = 5):
    global namespace, current_job
    namespace = namespace_
    current_job = job_name

    setup(job_name)

    consecutive_failures = 0

    blacklisted_confs = ExpireCollection()
    cluster_name = "{0}-{1}-{2}".format(cluster_name_prefix, "classic" if disable_vpc else "vpc", env)

    full_conf = None
    entity_id = "{0}-runner-{1}".format(cluster_name_prefix, uuid.uuid1())

    def inotify(message, severity="CRITICAL"):
        return notify(message, severity, entity_id=entity_id)

    while True:
        try:
            full_conf = ensure_cluster(cluster_name, cluster_options, full_conf,
                                       blacklisted_confs, collect_results_dir, entity_id, disable_vpc, security_group=security_group, tag=tag)
            while True:
                success, consecutive_failures = run_job(cluster_name, job_name, full_conf, collect_results_dir,
                                                        consecutive_failures, max_errors_on_healthy_cluster, entity_id)
                if not success:
                    break

        except Exception as e:
            log.exception('Completely unknown exception')
            inotify("Completely unknown exception\nTime to panic. Exception is: " + traceback.format_exc())


def run_once(collect_results_dir, namespace_, job_name, cluster_name_prefix, cluster_options,
             disable_vpc=False, security_group=default_security_group, tag=[], max_errors_on_healthy_cluster = 3):

    global namespace, current_job
    namespace = namespace_
    current_job = job_name

    setup(job_name)

    consecutive_failures = 0

    blacklisted_confs = ExpireCollection()
    cluster_name = "{0}-{1}-{2}".format(cluster_name_prefix, "classic" if disable_vpc else "vpc", env)

    full_conf = None
    entity_id = "{0}-runner-{1}".format(cluster_name_prefix, uuid.uuid1())
    def inotify(message, severity="CRITICAL"):
        return notify(message, severity, entity_id=entity_id)

    while True:
        try:
            full_conf = ensure_cluster(cluster_name, cluster_options, full_conf,
                                       blacklisted_confs, collect_results_dir, entity_id, disable_vpc, security_group=security_group, tag=tag)
            success, consecutive_failures = run_job(cluster_name, job_name, full_conf, collect_results_dir,
                                                    consecutive_failures, max_errors_on_healthy_cluster, entity_id)
            if success:
                break
            elif consecutive_failures >= max_errors_on_healthy_cluster:
                message = 'We had {0} consecutive failures of maximum of {1}.\nTime to panic. All processing is halted!'.format(consecutive_failures, max_errors_on_healthy_cluster)
                log.error(message)
                inotify(message)
                break
        except Exception as e:
            log.exception('Completely unknown exception')
            inotify("Completely unknown exception\nTime to panic. Exception is: " + traceback.format_exc())
    destroy_all_clusters(cluster_name)


def sitemap_generation(collect_results_dir, disable_vpc = False, security_group = default_security_group):
    run_once(collect_results_dir,
            "sitemaps",
             job_name="SitemapXMLSetup",
             cluster_name_prefix="sitemap-generation",
             cluster_options=search_generation_cluster_options,
             disable_vpc=disable_vpc,
             security_group=security_group,
             tag=["chaordic:role=gen.sitemap"])


def search_etl(collect_results_dir, disable_vpc = False, security_group = default_security_group):
    run_once(collect_results_dir,
             "search_etl",
             job_name="SearchETLSetup",
             cluster_name_prefix="search-etl-generation",
             cluster_options=search_generation_cluster_options,
             disable_vpc=disable_vpc,
             security_group=security_group,
             tag=["chaordic:role=gen.search-etl-generation"])

class ExpireCollection:
    """
    >>> c = ExpireCollection(timeout=0.5)
    >>> import time
    >>> c.add('something')
    >>> len(c)
    1
    >>> 'something' in c
    True
    >>> time.sleep(0.6)
    >>> len(c)
    0
    """
    def __init__(self, timeout=60*60*3):
        import collections
        self.timeout = timeout
        self.events = collections.deque()

    def add(self, item):
        import threading
        self.events.append(item)
        threading.Timer(self.timeout, self.expire).start()

    def __len__(self):
        return len(self.events)

    def expire(self):
        """Remove any expired events
        """
        self.events.popleft()

    def __str__(self):
        return str(self.events)

    def __contains__(self, elem):
        return elem in self.events


def object_to_base64(obj):
    return base64.b64encode(pickle.dumps(obj))

def base64_to_object(b64):
    return pickle.loads(base64.b64decode(b64))

if __name__ == '__main__':
    import doctest
    doctest.testmod()
    parser = ArghParser()
    parser.add_commands([sitemap_generation, transaction_etl])
    parser.dispatch()
