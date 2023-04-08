import json
import subprocess
import sys

def kill_cluster(config):
    if not config['run_locally']:
        for i in range(config['repl_factor'] + config['num_shards']):
            name = 'node%s' % i
            hostname = config['host_format_str'] % (name, config['experiment_name'], config['project_name'])
            
            kill_remote_process_by_name("repl_store", config['user'], hostname)

            for i in range(config['num_chain']):
                name = 'node%s' % (i + config['repl_factor'] + config['num_shards'])
                hostname = config['host_format_str'] % (name, config['experiment_name'], config['project_name'])                                
                kill_remote_process_by_name("txn_manager", config['user'], hostname)
    else:
        run_local_command_sync(kill_remote_process_by_name_cmd("repl_store"))
        run_local_command_sync(kill_remote_process_by_name_cmd("txn_manager"))
                                   

def kill_remote_process_by_name_cmd(remote_process_name):
    cmd = 'pkill %s' % (remote_process_name)
    return cmd


def kill_remote_process_by_name(remote_process_name, remote_user, remote_host, kill_args):
    run_remote_command_sync(kill_remote_process_by_name_cmd(remote_process_name,
                                                            kill_args), remote_user, remote_host)

def run_remote_command_sync(command, remote_user, remote_host):
    print("{}@{}: {}".format(remote_user, remote_host, command))
    return subprocess.run(ssh_args(command, remote_user, remote_host),
                          stdout=subprocess.PIPE, universal_newlines=True).stdout

def run_local_command_sync(command):
    print(command)
    subprocess.run(command, stdout=subprocess.PIPE, universal_newlines=True, shell=True)

if __name__ == '__main__':
    if len(sys.argv) == 0:
        sys.stderr.write('Usage: python3 %s <config_file>\n' % sys.argv[0])
        sys.exit(1)
    with open(sys.argv[1]) as f:
        config = json.load(f)
        kill_cluster(config)
