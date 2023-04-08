import json
import time
import sys
import subprocess
import os

def get_init_cluster(config):
    commands = []
    if config['total_num_servers'] < (config['repl_factor'] * config['num_shards']) + config['num_chain']:
        sys.stderr.write("Not enough addresses for the number of nodes")
        sys.exit(1)
    if config['num_chain'] < 2:
        sys.stderr.write("Singleton chain not supported")
        sys.exit(1)

    raft_servers = []
    chain_servers = []
    port = 5001
    # build and start synchronously
    for i in range(config['repl_factor'] * config['num_shards']):
        addrs = {}
        name = 'node%s' % i
        hostname = config['host_format_str'] % (name, config['experiment_name'], config['project_name'])
        # get experiment IP address
        ip = get_ip_for_server_name(name, config['user'], hostname, config['run_locally'])
        if not config['run_locally']:
            addrs['cluster'] = ip + ":" + config['cluster_port']
            addrs['control'] = ip + ":" + config['control_port']
        else:
            addrs['cluster'] = ip + ":" + str(port)
            port += 1
            addrs['control'] = ip + ":" + str(port)
            port += 1

        raft_servers.append(addrs)
        
        # run binary
        if not config['run_locally']:
            raft_bin_command = ["cd", config['experiment_path'], ";",
                                "cargo run --release --bin repl_store -- " +
                                addrs['cluster'] + " " + addrs['control']]
            run_remote_command_async(raft_bin_command, config['user'], hostname)
        else:
            raft_bin_command = ["cargo run --release --bin repl_store -- "+
                                addrs['cluster'] +  " " + addrs['control'] + " > test_output/raft_" + str(i) + ".log"]
            run_local_command_async(raft_bin_command)
            port += 1
        
        
    for i in range(config['num_chain']):
        addrs = {}
        name = 'node%s' % (i + config['repl_factor'] + config['num_shards'])
        hostname = config['host_format_str'] % (name, config['experiment_name'], config['project_name'])

        # get experiment ip
        if not config['run_locally']:
            addrs['cluster'] = ip + ":" + config['cluster_port']
            addrs['control'] = ip + ":" + config['control_port']
        else:
            addrs['cluster'] = ip + ":" + str(port)
            port += 1
            addrs['control'] = ip + ":" + str(port)
            port += 1
        chain_servers.append(addrs)

        # run binary
        if not config['run_locally']:
            chain_bin_command = ["cd", config['experiment_path'], ";",
                                 "cargo run --release --bin txn_manager -- " + 
                                 addrs['cluster'] + " " + addrs['control']]
            run_remote_command_async(chain_bin_command, config['user'], hostname)
        else:
            chain_bin_command = ["cargo run --release --bin txn_manager -- " + 
                                 addrs['cluster'] + " " + addrs['control'] + "> test_output/chain" + str(i)+".log"]
            run_local_command_async(chain_bin_command)  
            port += 1          

    time.sleep(5)
    
    # modify json for control plane config spec
    config_file_path = config['experiment_path'] + "/temp/control_config.json"
    os.makedirs(os.path.dirname(config_file_path), exist_ok=True)
    with open(config_file_path, "w") as control_conf:
        new_conf = {}
        new_conf['cluster_port'] = config['cluster_port']
        new_conf['control_port'] = config['control_port']
        new_conf['num_shards'] = config['num_shards']
        new_conf['repl_factor'] = config['repl_factor']
        new_conf['raft_servers'] = raft_servers
        new_conf['chain_servers'] = chain_servers
        json.dump(new_conf, control_conf)

    # send to control and start
    control_hostname = config['host_format_str']%('control', config['experiment_name'], config['project_name'])
    control_bin_command = ["cd " + config['experiment_path']+ "; "+
                       "cargo run --release --bin control -- " + config_file_path + " > test_output/control.log"]
    
    if not config['run_locally']:
        send_to_remote(config['user'], control_hostname, config_file_path, config_file_path)
        run_remote_command_sync(control_bin_command, config['user'], control_hostname)
    else:
        run_local_command_sync(control_bin_command)
    
    # create clients, static round robin load balancing over chain
    client_port = 8001
    client_chain = chain_servers[:-1]
    for i in range(config['num_client_machines']):
        name = 'client%s' % i
        hostname = config['host_format_str'] % (name, config['experiment_name'], config['project_name'])
        ip = get_ip_for_server_name(name, config['user'], hostname, config['run_locally'])
        for j in range(config['clients_per_machine']):
            ind = j % len(client_chain)
            gen_client_config(i, j, ip + ":" + str(client_port), client_chain[0]['cluster'], client_chain[ind]['cluster'] ,config)
            client_port += 1
        if not config['run_locally']:
            client_port = 8001

    
def gen_client_config(machine, client, my_addr, head_addr, chain_addr, config):   
    client_config_path = config['experiment_path'] + '/temp/client_config_%s_%s.json'%(machine, client)
    with open(client_config_path, "w") as client_conf:
        new_conf = {}
        new_conf['skew'] = config['skew']
        new_conf['num_keys'] = config['num_keys']    
        new_conf['my_addr'] = my_addr
        new_conf['head_addr'] = head_addr
        new_conf['chain_addr'] = chain_addr
        json.dump(new_conf, client_conf)

def ssh_args(command, remote_user, remote_host):
    return ["ssh", '-o', 'StrictHostKeyChecking=no',
            '-o', 'ControlMaster=auto',
            '-o', 'ControlPersist=2m',
            '-o', 'ControlPath=~/.ssh/cm-%r@%h:%p',
            '%s@%s' % (remote_user, remote_host), command]

def run_local_command_sync(command):
    print(command)
    subprocess.run(command, stdout=subprocess.PIPE, universal_newlines=True, shell=True)

def run_local_command_async(command):
    print(command)
    return subprocess.Popen(command, universal_newlines=True, shell=True)

def run_remote_command_sync(command, remote_user, remote_host):
    print("{}@{}: {}".format(remote_user, remote_host, command))
    return subprocess.run(ssh_args(command, remote_user, remote_host),
                          stdout=subprocess.PIPE, universal_newlines=True, shell=True).stdout


def run_remote_command_async(command, remote_user, remote_host, detach=True):
    print("{}@{}: {}".format(remote_user, remote_host, command))
    if detach:
        command = '(%s) >& /dev/null & exit' % command
    return subprocess.Popen(ssh_args(command, remote_user, remote_host))
    

def kill_remote_process_by_name_cmd(remote_process_name, kill_args):
    cmd = 'pkill%s %s' % (kill_args, remote_process_name)
    return cmd


def kill_remote_process_by_name(remote_process_name, remote_user, remote_host, kill_args):
    run_remote_command_sync(kill_remote_process_by_name_cmd(remote_process_name,
                                                            kill_args), remote_user, remote_host)
    
def get_ip_for_server_name(server_name, remote_user, remote_host, is_local):
    if is_local:
        return "127.0.0.1"
    else:
        return run_remote_command_sync('getent hosts %s | awk \'{ print $1 }\'' % server_name, remote_user, remote_host).rstrip()

def send_to_remote(remote_user, remote_host, local_path, remote_path):
    command = ["scp", "-r", "-p", "%s" % local_path, "%s@%s:%s" % (remote_user, remote_host, remote_path)]
    run_remote_command_sync(command, remote_user, remote_host)


if __name__ == '__main__':
    if len(sys.argv) == 0:
        sys.stderr.write('Usage: python3 %s <config_file>\n' % sys.argv[0])
        sys.exit(1)
    with open(sys.argv[1]) as f:
        config = json.load(f)
        get_init_cluster(config)
