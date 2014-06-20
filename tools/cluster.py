#!/usr/bin/env python

"""
Spark cluster management script.

This line is to make pylint happy

"""

from argh import ArghParser, CommandError
from argh.decorators import named, arg
import subprocess
from subprocess import check_output, check_call
from itertools import chain
from utils import tag_instances, get_masters
import os
import sys
from datetime import datetime
import time
import logging
import getpass


log = logging.getLogger()
log.setLevel(logging.INFO)
#log.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler = log.handlers[0]
handler.setFormatter(formatter)
log.addHandler(handler)

script_path = os.path.dirname(os.path.realpath(__file__))

default_instance_type = 'r3.xlarge'
default_spot_price = '0.05'
default_worker_instances = '1'
default_master_instance_type = 'c3.large'
default_region = 'us-east-1'
default_zone = default_region + 'b'
default_key_id = 'mail_chaordic_key'
default_ami = 'ami-35b1885c'  # HVM AMI
default_env = 'prod'
default_spark_version = '1.0.0'
default_remote_user = 'ec2-user'
default_remote_control_dir = '/tmp/Ignition'
default_collect_results_dir = '/tmp'
default_user_data = os.path.join(script_path, 'scripts', 'S05mount-disks')

master_post_create_commands = [
'sudo', 'yum', '-y', 'install', 'tmux'
]

def logged_call_base(func, args, tries):
    for i in range(tries - 1):
        log.debug('Calling with retry: %s', args)
        try:
            return func(args)
        except Exception as e:
            log.warn('Got exception {0}, retrying...'.format(e))
    log.debug('Calling: %s', args)
    return func(args)

# We suppose we are in a sub sub director of the root (like: root-project/core/tools/cluster.py)
def get_project_path():
    return os.path.realpath(os.path.join(script_path, '../..'))


def logged_call_output(args, tries=1):
    return logged_call_base(check_output, args, tries)


def logged_call(args, tries=1):
    return logged_call_base(check_call, args, tries)


def ssh_call(user, host, key_file, args=(), allocate_terminal=True, get_output=False):
    base = ['ssh', '-q']
    if allocate_terminal:
        base += ['-t']
    base += ['-i', key_file,
             '-o', 'StrictHostKeyChecking=no',
             '{0}@{1}'.format(user, host)]
    base += args
    if get_output:
        return logged_call_output(base)
    else:
        return logged_call(base)

def chdir_to_ec2_script_and_get_path():
    ec2_script_base = os.path.join(script_path, 'spark-ec2')
    os.chdir(ec2_script_base)
    ec2_script_path = os.path.join(ec2_script_base, 'spark_ec2.py')
    return ec2_script_path


def call_ec2_script(args):
    ec2_script_path = chdir_to_ec2_script_and_get_path()
    return logged_call([ec2_script_path] + args)


def cluster_exists(cluster_name):
    try:
        get_master(cluster_name)
        return True
    except Exception as e:
        return False


def launch(cluster_name, slaves, key_file, team, env=default_env,
           key_id=default_key_id, region=default_region,
           zone=default_zone, instance_type=default_instance_type,
           spot_price=default_spot_price,
           user_data = default_user_data,
           master_instance_type=default_master_instance_type,
           wait_time='180', hadoop_major_version='2',
           worker_instances=default_worker_instances, retries_on_same_cluster=3,
           max_clusters_to_recreate=5,
           remote_user=default_remote_user,
           resume=False, worker_timeout=240,
           spark_version=default_spark_version, ami=default_ami):

    if cluster_exists(cluster_name) and not resume:
        raise CommandError('Cluster already exists, pick another name or resume the setup using --resume')

    for j in range(max_clusters_to_recreate):
        log.info('Creating new cluster {0}, try {1}'.format(cluster_name, j+1))
        success = False
        resume_param = ['--resume'] if resume else []
        for i in range(retries_on_same_cluster):
            log.info('Running script, try %d of %d', i + 1, retries_on_same_cluster)
            try:
                call_ec2_script(['--ami', ami,
                            '--identity-file', key_file,
                            '--key-pair', key_id,
                            '--slaves', slaves,
                            '--spot-price', spot_price,
                            '--region', region,
                            '--zone', zone,
                            '--instance-type', instance_type,
                            '--master-instance-type', master_instance_type,
                            '--wait', wait_time,
                            '--hadoop-major-version', hadoop_major_version,
                            '--worker-instances', worker_instances,
                            '--master-opts', '-Dspark.worker.timeout={0}'.format(worker_timeout),
                            '-v', spark_version,
                            '--user-data', user_data,
                            'launch', cluster_name] + resume_param)
                success = True
            except subprocess.CalledProcessError as e:
                resume_param = ['--resume']
                log.warn('Failed with: %s', e)
            tag_instances(cluster_name, {'team': team,
                                        'env': env,
                                        'spark_cluster_name': cluster_name,
                                        'name': cluster_name})

            # TODO: use a more elaborate test here
            success = success and cluster_exists(cluster_name)
            if success:
                break

        if success:
            ssh_master(cluster_name, key_file, remote_user, *master_post_create_commands)
            return get_master(cluster_name)
        else:
            log.warn('Destroying unsuccessful cluster')
            destroy(cluster_name=cluster_name, region=region)
    raise CommandError('Failed to created cluster {} after failures'.format(cluster_name))


def destroy(cluster_name, region=default_region):
    ec2_script_path = chdir_to_ec2_script_and_get_path()
    p = subprocess.Popen([ec2_script_path, 'destroy', cluster_name, '--region', region],
                         stdin=subprocess.PIPE,
                         stdout=sys.stdout, universal_newlines=True)
    p.communicate('y')


def get_master(cluster_name):
    masters = get_masters(cluster_name)
    if not masters:
        raise CommandError("No master on {}".format(cluster_name))
    return masters[0].public_dns_name


def ssh_master(cluster_name, key_file, user='ec2-user', *args):
    master = get_master(cluster_name)
    ssh_call(user=user, host=master, key_file=key_file, args=args)


@arg('job-mem', help='The amount of memory to use for this job (like: 80G)')
@arg('--master', help="This parameter overrides the master of cluster-name")
@arg('--disable-tmux', help='Do not use tmux. Warning: many features will not work without tmux. Use only if the tmux is missing on the master.')
@arg('--detached', help='Run job in background, requires tmux')
@arg('--destroy-cluster', help='Will destroy cluster after finishing the job')
@named('run')
def job_run(cluster_name, job_name, job_mem, key_file, disable_tmux=False,
            detached=False, notify_on_errors=False, yarn=False,
            job_user = getpass.getuser(),
            remote_user=default_remote_user, utc_job_date=None, job_tag=None,
            disable_wait_completion=False, collect_results_dir=default_collect_results_dir,
            remote_control_dir = default_remote_control_dir,
            remote_path=None, master=None,
            destroy_cluster=False):

    utc_job_date_example = '2014-05-04T13:13:10Z'
    if utc_job_date and len(utc_job_date) != len(utc_job_date_example):
        raise CommandError('UTC Job Date should be given as in the following example: {}'.format(utc_job_date_example))
    disable_tmux = disable_tmux and not detached
    wait_completion = not disable_wait_completion or destroy_cluster
    master = master or get_master(cluster_name)

    project_path = get_project_path()
    project_name = os.path.basename(project_path)
    # Use job user on remote path to avoid too many conflicts for different local users
    remote_path = remote_path or '/home/%s/%s.%s' % (default_remote_user, job_user, project_name)
    remote_app_path = remote_path
    notify_param = 'yes' if notify_on_errors else 'no'
    yarn_param = 'yes' if yarn else 'no'
    job_date = utc_job_date or datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    job_tag = job_tag or job_date.replace(':', '_').replace('-', '_').replace('Z', 'UTC')
    tmux_wait_command = ';(echo Press enter to keep the session open && /bin/bash -c "read -t 5" && sleep 7d)' if not detached else ''
    tmux_arg = ". /etc/profile; . ~/.profile;tmux new-session {detached} -s spark.{job_name}.{job_tag} '{remote_app_path}/remote_hook.sh {job_name} {job_date} {job_tag} {job_user} {remote_control_dir} {spark_mem} {yarn_param} {notify_param} {tmux_wait_command}'".format(
        job_name=job_name, job_date=job_date, job_tag=job_tag, job_user=job_user, remote_control_dir=remote_control_dir, remote_app_path=remote_app_path, spark_mem=job_mem, detached='-d' if detached else '', yarn_param=yarn_param, notify_param=notify_param, tmux_wait_command=tmux_wait_command)
    non_tmux_arg = ". /etc/profile; . ~/.profile;{remote_app_path}/remote_hook.sh {job_name} {job_date} {job_tag} {job_user} {remote_control_dir} {spark_mem} {yarn_param} {notify_param}".format(
        job_name=job_name, job_date=job_date, job_tag=job_tag, job_user=job_user, remote_control_dir=remote_control_dir, remote_app_path=remote_app_path, spark_mem=job_mem, yarn_param=yarn_param, notify_param=notify_param)
    rsync_args = ['rsync', '--timeout', '60']
    rsync_args += reduce(chain, (['--exclude', i]
                for i in ('.git', 'target', 'tools',
                          '.idea', '.idea_modules', '.lib')))
    rsync_args += ['--delete', '-azvP', project_path + '/',
             '-e', 'ssh -i {} -o StrictHostKeyChecking=no'.format(key_file),
             '{0}@{1}:{2}'.format(remote_user, master, remote_path)]
    logged_call(rsync_args, tries=3)

    if disable_tmux:
        ssh_call(user=remote_user, host=master, key_file=key_file, args=[non_tmux_arg], allocate_terminal=False)
    else:
        ssh_call(user=remote_user, host=master, key_file=key_file, args=[tmux_arg], allocate_terminal=True)

    if wait_completion:
        failed = False
        try:
            wait_for_job(cluster_name=cluster_name, job_name=job_name,
                     job_tag=job_tag, key_file=key_file, master=master,
                     remote_user=remote_user, remote_control_dir=remote_control_dir,
                     collect_results_dir=collect_results_dir)
        except JobFailure as e:
            failed = True
            log.warn('Job failed with: {}'.format(e))
        if destroy_cluster:
            log.info('Destroying cluster as requested')
            destroy(cluster_name)
    return (job_name, job_tag)

@named('attach')
def job_attach(cluster_name, key_file, job_name=None, job_tag=None,
               master=None, remote_user=default_remote_user):

    master = master or get_master(cluster_name)

    args = ['tmux', 'attach']
    if job_name and job_tag:
        args += ['-t', 'spark.{0}.{1}'.format(job_name, job_tag)]

    ssh_call(user=remote_user, host=master, key_file=key_file, args=args)


class JobFailure(Exception): pass


@named('wait-for')
def wait_for_job(cluster_name, job_name, job_tag, key_file,
                 master=None, remote_user=default_remote_user,
                 remote_control_dir=default_remote_control_dir,
                 collect_results_dir=default_collect_results_dir,
                 job_timeout_minutes=0, max_failures=10, seconds_to_sleep=60):

    master = master or get_master(cluster_name)

    job_with_tag = '{job_name}.{job_tag}'.format(job_name=job_name, job_tag=job_tag)
    log.info('Will wait remote status for job: {job_with_tag}'.format(job_with_tag=job_with_tag))

    job_control_dir = '{remote_control_dir}/{job_with_tag}'.format(remote_control_dir=remote_control_dir, job_with_tag=job_with_tag)
    ssh_call_check_status = [
                '''([ ! -e {path} ] && echo LOSTCONTROL) ||
                   ([ -e {path}/RUNNING ] && ps -p $(cat {path}/RUNNING) >& /dev/null && echo RUNNING) ||
                   ([ -e {path}/SUCCESS ] && echo SUCCESS) ||
                   ([ -e {path}/FAILURE ] && echo FAILURE) ||
                   echo KILLED'''.format(path=job_control_dir)
                ]
    failures = 0
    last_failure = None
    start_time = time.time()
    while True:
        try:
            output = (ssh_call(user=remote_user, host=master, key_file=key_file,
                               args=ssh_call_check_status, get_output=True) or '').strip()
            if output == 'SUCCESS':
                log.info('Job finished successfully!')
                break # TODO: Save remaining files, notify on errors
            elif output == 'FAILURE':
                log.error('Job failed...')
                raise JobFailure('Job failed...') # TODO: Save remaining files, notify on errors
            elif output == 'LOSTCONTROL':
                log.error('''No control directory found for the job. Possible explanations:
                          1) The given job name and tag are wrong
                          2) The given master server is wrong
                          3) Something is messing around with the server (rebooting it or deleting files)
                          4) The script has a bug (I really doubt ;)''')
                raise JobFailure('Lost control...')# TODO: notify
            elif output == 'KILLED':
                log.warn('Job has been killed before finishing')
                break # TODO: decide what to do
            elif output == 'RUNNING':
                log.info('Job is running...')
            else:
                log.warn('Received unexpected response while checking job status: {}'.format(output))
                failures += 1
                last_failure = 'Unexpected response: {}'.format(output)
        except subprocess.CalledProcessError as e:
            failures += 1
            log.warn('Got exception> {}'.format(e))
            last_failure = 'Exception: {}'.format(e)
        if failures > max_failures:
            log.error('Too many failures while checking job status, the last one was {}'.format(e))
            raise JobFailure('Too many failures')
        if job_timeout_minutes > 0 and (time.time() - start_time) / 60 >= job_timeout_minutes:
            raise JobFailure('Timed out')
        log.debug('Sleeping for {} seconds before checking new status'.format(seconds_to_sleep))
        time.sleep(seconds_to_sleep)


@named('kill')
def kill_job(cluster_name, job_name, job_tag, key_file,
             master=None, remote_user=default_remote_user,
             remote_control_dir=default_remote_control_dir):

    master = master or get_master(cluster_name)

    job_with_tag = '{job_name}.{job_tag}'.format(job_name=job_name, job_tag=job_tag)
    job_control_dir = '{remote_control_dir}/{job_with_tag}'.format(remote_control_dir=remote_control_dir, job_with_tag=job_with_tag)

    session_name = 'spark.{job_with_tag}'.format(job_with_tag=job_with_tag)
    ssh_call(user=remote_user, host=master, key_file=key_file,
        args=['''{
            pid=$(cat %s/RUNNING)
            children=$(pgrep -P $pid)
            sudo kill $pid $children
        }''' % job_control_dir
        ])


@named('killall')
def killall_jobs(cluster_name, key_file,
             master=None, remote_user=default_remote_user,
             remote_control_dir=default_remote_control_dir):
    master = master or get_master(cluster_name)
    ssh_call(user=remote_user, host=master, key_file=key_file,
            args=[
            '''for i in {remote_control_dir}/*/RUNNING; do
                pid=$(cat $i)
                children=$(pgrep -P $pid)
                sudo kill $pid $children || true
            done'''.format(remote_control_dir=remote_control_dir)
            ])




parser = ArghParser()
parser.add_commands([launch, destroy, get_master, ssh_master])
parser.add_commands([job_run, job_attach, wait_for_job, kill_job, killall_jobs], namespace="jobs")

if __name__ == '__main__':
    parser.dispatch()
