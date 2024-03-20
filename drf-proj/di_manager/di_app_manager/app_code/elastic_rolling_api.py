from di_app_manager.app_code.kafka_rolling_api import *
import socket
import timeit


@retry(stop=(stop_after_attempt(10)))
def get_elastic_cluster_size(task_id, host):
    """
    Get the cluster size
    :param task_id:
    :param host: The target host of elastic node we will use to connect and execute curl
    :return: output: the cluster size
    """
    try:
        logging.info('Now getting the status of elastic cluster ie green/yellow or red')
        cfg_results = getdefaultvalues()
        elastic_user = cfg_results.get('elastic_user')
        elastic_pass = cfg_results.get('elastic_pass')
        elastic_port = cfg_results.get('elastic_port')
        command = 'curl -k -u ' + elastic_user + ':' + elastic_pass + ' https://127.0.0.1:' + str(
            elastic_port) + '/_cluster/stats/'
        logging.debug('Host: ' + host + ' ,executing: ' + str(command))
        output, exit_code = remote_ssh_command_execution(task_id, host, command)
        logging.debug('executed with exit_code: ' + str(exit_code))
        if exit_code != 0:
            return None
        output = json.loads(output)
        output = (output['nodes']['count']['data'])
        logging.debug('the return output: ' + str(output))
        return output
    except Exception as e:
        logging.error('error: ' + str(e))


@retry(stop=(stop_after_attempt(10)))
def get_elastic_cluster_status(task_id, host):
    """
    Get the current status of elastic (green/yellow/red)
    :param task_id:
    :param host: The target host of elastic node we will use to connect and execute curl
    :return: output: green/yellow/red
    """
    try:
        logging.info('Now getting the status of elastic cluster ie green/yellow or red')
        cfg_results = getdefaultvalues()
        elastic_user = cfg_results.get('elastic_user')
        elastic_pass = cfg_results.get('elastic_pass')
        elastic_port = cfg_results.get('elastic_port')
        command = 'curl -s -k -u ' + elastic_user + ':' + elastic_pass + ' https://127.0.0.1:' + str(
            elastic_port) + '/_cluster/stats/'
        logging.debug('Host: ' + host + ' ,executing: ' + str(command))
        output, exit_code = remote_ssh_command_execution(task_id, host, command)
        logging.debug('executed with exit_code: ' + str(exit_code))
        if exit_code != 0:
            return 'N/A'
        output = json.loads(output)
        output = (output['status'])
        logging.debug('the return output: ' + str(output))
        return output
    except Exception as e:
        logging.error('error: ' + str(e))

def check_shards_for_hosts(func):
    def wrapper(task_id, hosts):
        shards_num = None
        while shards_num is None:
            for ip_address in hosts:
                shards_num = func(task_id, ip_address)
                logging.info(f"shards number on node \033[1;33;40m {ip_address} = {shards_num}\033[1;32;40m")
            return 0
    return wrapper

@check_shards_for_hosts
def get_elastic_shards_status(task_id, host):
    """
    Get the current shards allocation status of elastic node
    :param task_id:
    :param host: The target host of elastic node we will use to connect and execute curl
    :return: output: number of shards on node
    """
    try:
        cfg_results = getdefaultvalues()
        elastic_user = cfg_results.get('elastic_user')
        elastic_pass = cfg_results.get('elastic_pass')
        elastic_port = cfg_results.get('elastic_port')
        command = 'curl -s -k -u ' + elastic_user + ':' + elastic_pass + ' https://127.0.0.1:' + str(
            elastic_port) + '/_cat/shards/_all?h=ip | grep -x ' + host + ' | wc -l'
        output, exit_code = remote_ssh_command_execution(task_id, host, command)
        while int(output) != 0:
            logging.info('sleeping for 50 seconds before re-checking shards_status')
            check_kill_status(task_id)
            time.sleep(10)
            output, exit_code = remote_ssh_command_execution(task_id, host, command)

        message = "Checking the number of shards is completed, there is no data on the node"
        logging.info(message)
        check_kill_status(task_id)
        return 0, str(message)    

    except Exception as message:
       logging.error('error: ' + str(message))
       return 1, str(message)

@retry(stop=(stop_after_attempt(10)))
def disable_shard_allocation(task_id, node_ip, action):
    """
    Disable/enable shard allocation on target cluster
    :param task_id:
    :param node_ip:
    :param action: can be null or primaries
    :return:
    """

    try:
        cfg_results = getdefaultvalues()
        elastic_pass = cfg_results.get('elastic_pass')
        elastic_user = cfg_results.get('elastic_user')
        elastic_port = cfg_results.get('elastic_port')
        if action == 'primaries':
            action = '"' + action + '"'
        shard_allocation = "curl -k -u " + elastic_user + ":" + elastic_pass + \
                           " -X PUT \"https://127.0.0.1:" + str(elastic_port) + \
                           "/_cluster/settings?pretty\" -H " "'Content-Type: application/json' -d' {\"persistent\":" \
                           " { \"cluster.routing.allocation.enable\": " + action + "}}'"
        logging.debug('About to execute: ' + str(shard_allocation))
        output, exit_code = remote_ssh_command_execution(task_id, node_ip, shard_allocation)
        logging.debug("Shard allocation exit_code : " + str(exit_code))
        if action == 'null':
            logging.info("Shard allocation enabled Cluster wise")
        elif action == 'primaries':
            logging.info("Shard allocation disabled Cluster wise")
    except Exception as e:
        logging.error('error: ' + str(e))


def check_remote_port_open(ip_address, port):
    """
    check if a port is valid in a remote server
    :param ip_address:
    :param port:
    :return: 0 if port is open, 1 if port is closed.
    """

    logging.info('Lets check if Port: ' + str(port) + ' at ip address: ' + str(ip_address) + ' is open')
    a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    location = (ip_address, port)
    result_of_check = a_socket.connect_ex(location)
    logging.debug('result_of_check: ' + str(result_of_check))
    if result_of_check == 0:
        logging.info('Port: ' + str(port) + ' at ip address: ' + str(ip_address) + ' is open!')
        return 0
    else:
        logging.info("Port still un-responsive, sleep for 60 seconds")
        time.sleep(60)
        return 1


def execute_elastic_rolling_action(task_id, payload=None, results_dict=None, stack="elastic"):
    """
    Accept request with project_id, cluster_name, action and target_mail_address, then perform rolling restart/replace
     for the entire cluster
    :param results_dict: None by default unless this is a continuation run
    :param task_id:
    :param payload: None if results_dict is provided and this is a continuation run
    :param stack: Should be only aerospike
    :return:
    """
    
    logging.info('This is my task_id: ' + str(task_id))
    project_id, project_name, target_mail_address, cluster_name, action, results_dict, full_results_dict = \
        rolling_action_parameters(task_id, payload, results_dict, stack)
    status_code = 200
    ip_address = None
    status = 'status: '

    try:
        # Verify operation is valid (replace only)
        action = action.lower()
        if action not in ['replace', 'restart', 'upload']:
            status = 'action should be only replace/restart or upload'
            logging.error('The selected action is not valid only action replace or restart are valid')
            status_code = 202
            update_task(str(task_id), 'Finished', str(status_code), str(status), str(results_dict))
            sys.exit(1)

        # Get elastic cluster size
        logging.info('Checking cluster size')
        results_dict_keys = full_results_dict.keys()
        cluster_size = len(results_dict_keys)-1
        logging.info("Cluster size found is: " + str(cluster_size))

        start = timeit.default_timer()
        nodes_rolled = 0
        for key, value in sorted(results_dict.items(), reverse=True):
            check_kill_status(task_id)
            logging.debug("key: " + str(key))
            logging.debug("value: " + str(value))
            if key != -9999:
                if results_dict[key]['progress'] != 'done':
                    # from the instance group, get the ip address
                    ip_address, instance_name = get_ip_address(task_id, project_id, value['instance group'],
                                                               value['zone'])

                    current_cluster_size = None
                    logging.info('Verify that the cluster size is valid.')
                    while str(current_cluster_size) != str(cluster_size):
                        current_cluster_size = get_elastic_cluster_size(task_id, ip_address)
                        logging.debug('cluster_size: ' + str(cluster_size))
                        logging.debug('current_cluster_size: ' + str(current_cluster_size))
                        if str(current_cluster_size) != str(cluster_size):
                            time.sleep(60)
                            check_kill_status(task_id)

                    # if this is not the first Iter or last iter
                    if nodes_rolled != 0 or (nodes_rolled == cluster_size - 1):
                        # Calculate time left and avg time per node.
                        stop = timeit.default_timer()
                        execution_time = stop - start
                        estimated_time, avg_node_formatted = calculate_work_time(
                            cluster_size, nodes_rolled, execution_time)
                        timing_status = 'The current end time estimate for the entire operation is: ' \
                                        + str(estimated_time) + '\nThe current Avg time per node estimate is: '\
                                        + str(avg_node_formatted) + '\n'
                    else:
                        timing_status = ''

                    # check that the cluster is in green status
                    logging.info('Lets check the elastic cluster status before we move forward')
                    elastic_cluster_status = None
                    while elastic_cluster_status != 'green':
                        # enable shard allocation
                        disable_shard_allocation(task_id, ip_address, 'null')
                        time.sleep(10)
                        elastic_cluster_status = get_elastic_cluster_status(task_id, ip_address)
                        logging.info('The current cluster status: ' + str(elastic_cluster_status))
                        if elastic_cluster_status != 'green':
                            logging.info('Cluster status is not green, sleeping before retry')
                            time.sleep(50)
                            check_kill_status(task_id)

                    if action == 'restart' or action == 'replace':
                        # disable shard allocation
                        disable_shard_allocation(task_id, ip_address, 'primaries')
                        command = "sudo service elasticsearch stop"
                        stop_output, stop_exit_code = remote_ssh_command_execution(task_id, ip_address, command)
                        logging.debug(stop_output)
                        logging.debug(stop_exit_code)

                    # if action is restart, restart the server, if replace then fo instance group replace command
                    if action == 'replace':
                        verify_output = 0
                        verify_counter = 0
                        # verify_output == 1 means there is an instance under the instance group in a created status
                        while verify_output != 1:
                            instance_group_action(task_id, value['instance group'], value['zone'], action.lower())
                            verify_output = instance_group_action_verify(task_id, value['instance group'], value['zone'])
                            verify_counter += 1
                            check_kill_status(task_id)
                            if verify_counter >= 2:
                                logging.info("Google Api is not nice, Lets try and resize the instance group.")
                                instance_group_resize(task_id, value['instance group'], value['zone'], 0)
                                logging.info('Lets wait for node the be deleted, 120 seconds sleep')
                                time.sleep(120)
                                instance_group_resize(task_id, value['instance group'], value['zone'], 1)
                                break
                        status = str(action) + ' of Instance group: ' + str(value['instance group']) + ', Node: ' \
                                 + str(instance_name) + \
                                 ' Finished, Waiting for cluster to re-balance before proceeding to the next node'
                        logging.info('Lets wait for node the reboot, 120 seconds sleep')
                        time.sleep(120)
                        check_kill_status(task_id)
                    if action == 'restart' or action == 'upload':
                        time.sleep(10)
                        logging.info('Now executing /root/upload.sh from CGS bucket.')
                        command = "sudo /root/upload.sh"
                        upload_output, upload_exit_code = remote_ssh_command_execution(task_id, ip_address, command)
                        status = ('upload exit_code: ' + str(upload_exit_code) + '\n' + str(upload_output))
                        logging.debug(status)
                        time.sleep(10)
                    if action == 'restart':
                        logging.info('Now starting the elasticsearch service')
                        command = "sudo service elasticsearch start"
                        start_output, start_exit_code = remote_ssh_command_execution(task_id, ip_address, command)
                        status = status + ('start service exit_code: ' + str(start_exit_code) +
                                           '\n' + str(start_output))
                        logging.debug(status)

                    check_kill_status(task_id)
                    results_dict[key]['progress'] = 'done'
                    nodes_rolled += 1

                    # Prepare and send email info about operation progress
                    status = timing_status + status

                    subject = "Elastic cluster: " + instance_name + '@' + cluster_name + " Rolling " + action +\
                              " is moving forward"
                    sendemail(target_mail_address, {"subject": subject, "body": str(status)})
                    # Update the PG repo about the operation progress
                    update_task(str(task_id), 'Running', 200, str(status), str(results_dict))

                else:
                    if results_dict[key] != -9999:
                        logging.info(value['instance group'] + ' previously handled, skipping')

        # Setting the IP to run commands after last node is replaced in order to set migration delay
        logging.info('Using the IP address of the last node')
        last_instance = get_instance_from_instance_group(task_id, results_dict[cluster_size-1]['instance group'],
                                                         results_dict[cluster_size-1]['zone'], project_id)
        last_ip = get_ip_from_instance(task_id, last_instance)
        last_ip = last_ip.strip()
        logging.info("last_ip is: " + str(last_ip) + " for instance: " + last_instance)

        current_cluster_size = None
        logging.info('Verify that the cluster size is valid.')
        while str(current_cluster_size) != str(cluster_size):
            current_cluster_size = get_elastic_cluster_size(task_id, last_ip)
            logging.debug('cluster_size: ' + str(cluster_size))
            logging.debug('current_cluster_size: ' + str(current_cluster_size))
            if str(current_cluster_size) != str(cluster_size):
                time.sleep(60)
                check_kill_status(task_id)

        # enable shard allocation
        disable_shard_allocation(task_id, last_ip, 'null')

        # check that the cluster is in green status
        logging.info('Lets check the elastic cluster status before we move forward')
        elastic_cluster_status = None
        while elastic_cluster_status != 'green':
            elastic_cluster_status = get_elastic_cluster_status(task_id, last_ip)
            logging.info('The current cluster status: ' + str(elastic_cluster_status))
            if elastic_cluster_status != 'green':
                logging.info('Cluster status is not green, sleeping before retry')
                time.sleep(60)
                check_kill_status(task_id)

        if action == 'restart' or action == 'upload':
            # Let's restore the upload file
            rollback_upload_file(task_id, last_ip, project_id, stack, cluster_name)

        status = str(action) + ' of cluster ' + str(cluster_name) + ' finished successfully.'
        message = "Rolling " + action + " of " + cluster_name + " finished successfully " + format_dict_for_email(results_dict)
        logging.info(message)
        subject = "Elastic cluster: " + cluster_name + " Rolling " + action + " finished successfully"
        sendemail(target_mail_address, {"subject": subject, "body": message})

        update_task(str(task_id), 'Finished', status_code, str(status)[:65535], str(results_dict))

    except Exception as e:

        # enable shard allocation
        try:
            disable_shard_allocation(task_id, ip_address, 'null')
        except Exception as e:
            logging.error('disable_shard_allocation failed.')
            logging.error(e)
        logging.error('error: ' + str(e))

        message = "Task ID: " + str(task_id) + "Rolling " + action + " of " + cluster_name + " Failed with error " + str(e) + " " + str(results_dict)
        logging.info(message)
        subject = "elastic cluster: " + cluster_name + " Rolling " + action + " Failed"
        sendemail(target_mail_address, {"subject": subject, "body": message})

        update_task(str(task_id), 'Finished', 500, str(e)[:65535], str(results_dict))

