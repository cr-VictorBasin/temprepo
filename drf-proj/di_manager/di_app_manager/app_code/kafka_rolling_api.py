import logging

import paramiko
import copy
import ast
import timeit
from di_app_manager.app_code.validation import *


@retry(stop=(stop_after_attempt(10)))
def cluster_instance_groups_dict(task_id, project_id, cluster_name, stack=None):
    """
    Produces a dict with the entire cluster info, this will be the bases for the entire operation
    :param task_id:
    :param project_id:
    :param cluster_name:
    :param stack: kafka/aerospike/elastic - if None instance_group_manager_prefix parameter will take default (kafka)
    :return:
    """

    try:
        logging.info('Let`s map the cluster instance groups and create the results dict')
        cfg_results = getdefaultvalues()
        instance_group_prefix = cfg_results.get(stack + 'instance_group_prefix')
        gcloud_path = cfg_results.get('gcloud_path')
        set_command = gcloud_path + " auth activate-service-account --key-file=/root/gcp_key.json; " + \
                      gcloud_path + " config set project " + project_id + "; "
        logging.info("instance_group_prefix is: " + instance_group_prefix)
        count_command = gcloud_path + " compute instance-groups list --filter=\"name~'" + \
                        instance_group_prefix + "-" + \
                        cluster_name + "-*'\" --sort-by=~name | awk '{print $1, $2}'|  tail -n +2"
        logging.debug('Executing: ' + set_command + count_command)
        process = subprocess.run(set_command + count_command, capture_output=True, text=True, shell=True)
        logging.debug(process.stdout)
        output = process.stdout
        logging.debug('output:' + str(output))
        output_error = process.stderr
        if process.returncode == 0:
            output_list = output.splitlines()
            output_dict = {}
            for line in output_list:
                data = line.split(" ")
                output_dict[data[0]] = data[1]
            results_dict = {}
            for key, value in output_dict.items():
                x_counter = int(key.split('-')[-1])
                results_dict[x_counter] = {}
                results_dict[x_counter]['instance group'] = key
                results_dict[x_counter]['zone'] = value
                results_dict[x_counter]['progress'] = None
            logging.debug('Created the following dict: ' + str(results_dict))
            return results_dict
        else:
            raise Exception(output_error)
    except Exception as e:
        logging.error(str(e))
        update_task(str(task_id), 'Retry', 500, str(e)[:65535])


@retry(stop=(stop_after_attempt(10)))
def instance_group_action(task_id, instance_group, zone, action):
    """
    Execute restart/replace (based on action) operation on target instance group
    :param task_id:
    :param instance_group:
    :param zone: the GCP zone where the instance group resides
    :param action: restart or replace only.
    :return:
    """
    try:
        cfg_results = getdefaultvalues()
        gcloud_path = cfg_results.get('gcloud_path')
        if action == 'replace':
            max_surge = '--max-surge=0  --replacement-method=substitute'
        else:
            max_surge = ' '
        command = gcloud_path + ' compute instance-groups managed rolling-action ' + action + ' ' + instance_group + \
                  ' --max-unavailable=1 ' + max_surge + ' --zone=' + zone
        logging.debug('About to execute: ' + str(command))
        process = subprocess.run(command, capture_output=True, text=True, shell=True)
        output = process.stdout
        if output:
            logging.debug('output: ' + str(output))
        output_error = process.stderr
        if process.returncode == 0:
            return output
        else:
            raise Exception(output_error)
    except Exception as e:
        logging.error(str(e))
        update_task(str(task_id), 'Retry', 500, str(e)[:65535])


@retry(stop=(stop_after_attempt(10)))
def get_instance_from_instance_group(task_id, instance_group, zone, project_id):
    """
    Get the instance that is running under the target instance group
    :param project_id:
    :param task_id:
    :param instance_group:
    :param zone: the GCP zone where the instance group resides
    :return:
    """
    try:
        cfg_results = getdefaultvalues()
        gcloud_path = cfg_results.get('gcloud_path')
        command = gcloud_path + " auth activate-service-account --key-file=/root/gcp_key.json; " + \
                  gcloud_path + " config set project " + project_id + "; " + \
                  gcloud_path + " compute instance-groups managed describe " + \
                  instance_group + " --zone " + zone + \
                  " |grep -i baseInstanceName | awk '{print $2}'"

        logging.debug('About to execute: ' + str(command))
        process = subprocess.run(command, capture_output=True, text=True, shell=True)
        output = process.stdout
        exit_code = process.returncode
        if output:
            logging.debug('output: ' + str(output))
        output_error = process.stderr
        if output_error:
            logging.debug('output_error: ' + str(output_error))
        if exit_code == 0:
            logging.debug('exit_code: ' + str(exit_code))
            return output
        else:
            raise Exception(output_error)
    except Exception as e:
        logging.error(str(e))
        update_task(str(task_id), 'Retry', 500, str(e)[:65535])


@retry(stop=(stop_after_attempt(10)))
def get_ip_from_instance(task_id, instance_name):
    """
    Get the IP of a target GCP instance (VM)
    :param task_id:
    :param instance_name:
    :return:
    """
    try:
        cfg_results = getdefaultvalues()
        gcloud_path = cfg_results.get('gcloud_path')
        command = gcloud_path + ' compute instances list --filter="name~^' + instance_name + '-" --format="value(networkInterfaces[0].networkIP)"'
        command = command.replace("\n", "")
        logging.debug('About to execute: ' + str(command))
        process = subprocess.run(command, capture_output=True, text=True, shell=True)
        output = process.stdout
        if output:
            logging.debug('output: ' + str(output))
        output_error = process.stderr
        if process.returncode == 0:
            output = output.strip()
            return output
        else:
            raise Exception(output_error)
    except Exception as e:
        logging.error(str(e))
        update_task(str(task_id), 'Retry', 500, str(e)[:65535])


@retry(stop=(stop_after_attempt(10)))
def remote_ssh_command_execution(task_id, host, command):
    """
    Connect to a host and remotely execute a command, return the command output and the exit_code
    :param task_id:
    :param host: the target host to execute the command at (via ssh)
    :param command: the command to be executed on the host target
    :return: output: stdout of the command executed
    :return: exit_code: the exit_code of the command executed
    """
    logging.info('About to execute this command: ' + str(command) + ' on host: ' + str(host))
    ssh_client = None
    try:
        cfg_results = getdefaultvalues()
        private_key_file = cfg_results.get('private_key_file')
        username = cfg_results.get('os_username')
        enable_paramiko_debug = cfg_results.get('enable_paramiko_debug')
        if enable_paramiko_debug == 'no':
            logging.getLogger("paramiko").setLevel(logging.WARNING)
        else:
            logging.getLogger("paramiko").setLevel(logging.DEBUG)
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
        while not ssh_client.get_transport():
            ssh_client.connect(host,
                               username=username,
                               timeout=60,
                               key_filename=private_key_file,
                               allow_agent=False,
                               look_for_keys=False,
                               compress=True)
            logging.debug('About to execute: ' + str(command))
            stdin, stdout, stderr = ssh_client.exec_command(command)
            exit_code = stdout.channel.recv_exit_status()
            output = ''
            for line in iter(stdout.readline, ""):
                output += (line.strip()) + '\n'
            if output != '':
                logging.debug('command result:' + str(output))
            logging.debug('command exit_code:' + str(exit_code))
            stdout.channel.shutdown_read()
            stdout.channel.close()
            return output, exit_code
    except TimeoutError:
        status = "TimeoutError, no SSH access probably the node haven't started yet."
        logging.error(status)
        return 10, 2
    except Exception as e:
        logging.error('error: ' + str(e))
        update_task(str(task_id), 'Retry', 500, str(e)[:65535])
        return 10, 2
    finally:
        if ssh_client:
            ssh_client.close()


def rolling_action_parameters(task_id, payload=None, results_dict=None, stack=None):
    """
    Main rolling Replace/Restart/Upload function for Kafka

    :param task_id:
    :param payload:
    :param results_dict:
    :param stack:
    :return:
    """
    target_mail_address = None
    first_node_ip = None
    full_results_dict = None
    try:
        if payload is not None:
            logging.info('This is my task_id: ' + str(task_id))
            project_id = GetKeyValueFromDict(payload.data, 'project_id')
            project_name = get_project_name_from_id(project_id)
            target_mail_address = GetKeyValueFromDict(payload.data, 'target_mail_address')
            cluster_name = GetKeyValueFromDict(payload.data, 'cluster_name')
            action = GetKeyValueFromDict(payload.data, 'action')
            include_list = GetKeyValueFromDict(payload.data, 'include_list')

            # Validate if project already exists
            return_code, return_msg = check_project_exists(project_id)
            if return_code == 0 and return_msg == 'not exists':
                status = 'GCP Project: ' + str(project_id) + ' does not exists'
                update_task(str(task_id), 'Finished', 202, str(status), None)

            results_dict = cluster_instance_groups_dict(task_id, project_id, cluster_name, stack + '_')
            full_results_dict = copy.deepcopy(results_dict)

            # if targeted rolling restart/replace was requested then filter and create new result_dict
            logging.debug(include_list)
            logging.debug(type(include_list))
            if include_list != "all":
                logging.info('Targeted rolling was requested.')
                results_dict = selective_cluster_dict(results_dict, include_list, task_id)

            # Create action task specific 9999 key
            results_dict[-9999] = {}
            results_dict[-9999]['project_id'] = project_id
            results_dict[-9999]['cluster_name'] = cluster_name
            results_dict[-9999]['action'] = action
            full_results_dict[-9999] = {}
            full_results_dict[-9999]['project_id'] = project_id
            full_results_dict[-9999]['cluster_name'] = cluster_name
            full_results_dict[-9999]['action'] = action
        else:
            logging.info('This a continuation run, task id is: ' + str(task_id))
            logging.info('Based on results_dict: ' + str(results_dict))
            task_info = GetKeyValueFromDict(results_dict, -9999)
            task_info = ast.literal_eval(task_info)
            project_id = GetKeyValueFromDict(task_info, 'project_id')
            project_name = get_project_name_from_id(project_id)
            cluster_name = GetKeyValueFromDict(task_info, 'cluster_name')
            action = GetKeyValueFromDict(task_info, 'action')

        logging.info('This is the final results_dict: ' + str(results_dict))
        return project_id, project_name, target_mail_address, cluster_name, action, results_dict, full_results_dict

    except Exception as e:
        logging.error('error: ' + str(e))
        return 1, str(e)


def execute_kafka_rolling_action(task_id, payload=None, results_dict=None, stack="kafka"):
    """
    Accept request with project_id, cluster_name, action and target_mail_address, then perform rolling restart/replace
     for the entire cluster
    :param results_dict: None by default unless this is a continuation run
    :param task_id:
    :param payload: None if results_dict is provided and this is a continuation run
    :param stack: kafka/aerospike/elastic
    :return:
    """

    logging.info('This is my task_id: ' + str(task_id))
    project_id, project_name, target_mail_address, cluster_name, action, results_dict, full_results_dict = \
        rolling_action_parameters(task_id, payload, results_dict, stack)
    status_code = 200

    logging.info('Checking cluster size')
    results_dict_keys = full_results_dict.keys()
    cluster_size = len(results_dict_keys)-1
    logging.info("Cluster size found is: " + str(cluster_size))

    status = 'Status: '
    first_node_name = None
    try:
        # Verify operation is valid (restart or replace)
        action = action.lower()
        if action not in ['restart', 'replace', 'upload']:
            status = 'action should be restart/replace or upload'
            status_code = 202
            update_task(str(task_id), 'Finished', status_code, str(status), str(results_dict))

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

                    # Save first node name for later use
                    if nodes_rolled == 0:
                        first_node_name = instance_name
                        logging.debug('first_node_name: ' + str(first_node_name))

                    # Let's make sure target node is up and running before moving forward
                    logging.info('Lets check if port 22 for ssh is open at kafka node: ' + str(instance_name))
                    while not port_check(ip_address, 22):
                        logging.info('port 22 un-available at node: ' + str(instance_name) + ' ip: ' + str(ip_address))
                        time.sleep(60)
                        check_kill_status(task_id)
                        if not port_check(ip_address, 22):
                            instance_name = get_instance_from_instance_group(task_id, value['instance group'],
                                                                             value['zone'], project_id)
                            ip_address = get_ip_from_instance(task_id, instance_name)
                            logging.info("The instance_name is: " + str(instance_name))
                            logging.info("The ip_address is: " + str(ip_address))

                    error, info = check_kafka_cluster_status(task_id, ip_address, cluster_name, target_mail_address)
                    if int(error) == 1:
                        logging.error(info)
                        update_task(str(task_id), 'Finished', 500, str(info), str(results_dict))
                        message = "Rolling " + action + " of " + cluster_name + " failed on " + ip_address + " with error: " + info + " " + format_dict_for_email(results_dict)
                        subject = stack + " cluster: " + cluster_name + " Rolling " + action + " Failed"
                        sendemail(target_mail_address, {"subject": subject, "body": message})

                        sys.exit(1)


                    # if this is not the first Iter or last iter
                    if nodes_rolled != 0 or (nodes_rolled == cluster_size - 1):
                        # Calculate time left and avg time per node.
                        stop = timeit.default_timer()
                        execution_time = stop - start
                        estimated_time, avg_node_formatted = calculate_work_time(
                            cluster_size, nodes_rolled, execution_time)
                        timing_status = 'The current end time estimate for the entire operation is: ' \
                                        + str(estimated_time) + '\nThe current Avg time per node estimate is: ' \
                                        + str(avg_node_formatted) + '\n'
                    else:
                        timing_status = ''

                    if action == 'restart' or action == 'replace':
                        command = "sudo service kafka stop"
                        stop_output, stop_exit_code = remote_ssh_command_execution(task_id, ip_address, command)
                        logging.debug(stop_output)
                        logging.debug(stop_exit_code)

                    # if action is restart, restart the server, if replace then fo instance group replace command
                    check_kill_status(task_id)
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
                        logging.info('Now starting the kafka service')
                        command = "sudo service kafka start"
                        start_output, start_exit_code = remote_ssh_command_execution(task_id, ip_address, command)
                        status = status + ('start service exit_code: ' + str(start_exit_code) +
                                           '\n' + str(start_output))
                        logging.debug(status)

                    logging.info(status)

                    check_kill_status(task_id)
                    results_dict[key]['progress'] = 'done'
                    nodes_rolled += 1
                    # Prepare and send email info about operation progress
                    status = timing_status + status
                    update_task(str(task_id), 'Running', status_code, str(status), str(results_dict))

                    subject = "Kafka instance/cluster: " + instance_name + '@' + cluster_name + " Rolling " + action +\
                              " is moving forward"
                    sendemail(target_mail_address, {"subject": subject, "body": str(status)})

                else:
                    if key != -9999:
                        logging.info(value['instance group'] + ' previously handled, skipping')

        # Let's check the cluster status before we finish the operation
        first_node_ip = get_ip_from_instance(task_id, first_node_name)
        logging.info("The first_node_name is: " + str(first_node_name))
        logging.info("The first_node_ip is: " + str(first_node_ip))
        error, info = check_kafka_cluster_status(task_id, first_node_ip, cluster_name, target_mail_address)

        if int(error) == 1:
            logging.error(info)
            update_task(str(task_id), 'Finished', 500, str(info), str(results_dict))
            message = "Rolling " + action + " of " + cluster_name + " failed with error: " + info + " " + format_dict_for_email(results_dict)
            subject = stack + " cluster: " + cluster_name + " Rolling " + action + " Failed"
            sendemail(target_mail_address, {"subject": subject, "body": message})

            sys.exit(1)

        if action == 'restart' or action == 'upload':
            # Let's restore the upload file
            rollback_upload_file(task_id, first_node_ip, project_id, stack, cluster_name)

        if status_code == 200:
            status = str(action) + ' of cluster ' + cluster_name + ' finished successfully.'
            logging.info("The status code is 200: " + status)

            update_task(str(task_id), 'Finished', status_code, str(status)[:65535], str(results_dict))
            message = "Rolling " + action + " of " + cluster_name + " finished successfully " + format_dict_for_email(results_dict)
            logging.info(message)
            subject = stack + " cluster: " + cluster_name + " Rolling " + action + " finished successfully"
            sendemail(target_mail_address, {"subject": subject, "body": message})

    except Exception as e:
        logging.error('error: ' + str(e))
        message = "Task ID: " + str(task_id) + " Rolling " + action + " of " + cluster_name + " Failed with error "\
                  + str(e) + " " + str(results_dict)
        logging.info(message)
        subject = stack + " cluster: " + cluster_name + " Rolling " + action + " Failed"
        sendemail(target_mail_address, {"subject": subject, "body": message})

        update_task(str(task_id), 'Finished', 500, str(e)[:65535], str(results_dict))


def format_dict_for_email(dict_text):
    if isinstance(dict_text, dict):
        string = ''
    for key, value in sorted(dict_text.items(), reverse=True):
        string += "\n" + str(key) + ": " + str(value)
    return str(string)

@retry(stop=(stop_after_attempt(10)))
def check_kafka_cluster_status(task_id, ip_address, cluster_name, target_mail_address):
    """
    Check if there are kafka under replicated partitions, will pend until the kafka is balanced.
    :param task_id:
    :param ip_address:
    :param cluster_name:
    :param target_mail_address:
    :return: Will send an email every hour indicating that the check is still in progress
    """
    try:
        logging.info("Checking kafka under-replicated partitions")
        command = "kafkat partitions --under-replicated| wc -l"
        output, exit_code = remote_ssh_command_execution(task_id, ip_address, command)
        logging.debug("kafkat output is " + str(output))
        logging.debug(exit_code)

        if int(output) == 0:
            message = "Kafkat is not working as expected please check kafkat on the machine"
            logging.error(message)
            return 1, str(message)

        elapsed = 0
        while int(output) > 1:
            check_kill_status(task_id)
            logging.info('sleeping for 60 seconds before re-checking cluster status')
            time.sleep(60)
            elapsed += 60
            output, exit_code = remote_ssh_command_execution(task_id, ip_address, command)
            logging.debug(output)
            logging.debug(exit_code)
            if int(output) == 0:
                message = "Kafkat is not working as expected please check kafkat on the machine"
                logging.info(message)
                return 1, str(message)


            elapsed_check = elapsed % 3600

            if elapsed_check == 0:
                subject = "Kafka cluster: " + cluster_name + " under-replicated check is running too long"
                body = "Kafka under replicated partitions check is running too long, check manually why it is stuck"
                sendemail(target_mail_address, {"subject": subject, "body": body})

        message = "Under-replicated partitions check completed, there are not any under-replicated partitions"
        logging.info(message)
        return 0, str(message)

    except Exception as e:
        logging.error('error: ' + str(e))
        return 1, str(e)


def get_ip_address(task_id, project_id, instance_group, zone):
    """
     get the ip address from the given instance_group, assume a single instance per instance group

    :param task_id:
    :param project_id:
    :param instance_group:
    :param zone:
    :return: return a valid ip address
    """
    try:
        ip_address = 'None'
        # only move ahead if the ip_address is valid
        while not is_valid_IP_Address(ip_address):
            instance_name = get_instance_from_instance_group(task_id, instance_group, zone,
                                                             project_id)
            ip_address = get_ip_from_instance(task_id, instance_name)
            logging.info("The instance_name is: " + str(instance_name))
            logging.info("The ip_address is: " + str(ip_address))
            if not is_valid_IP_Address(ip_address):
                logging.debug('invalid ip address, sleeping + retrying')
                time.sleep(30)
                check_kill_status(task_id)
        return ip_address, instance_name
    except Exception as e:
        logging.error('error: ' + str(e))



def rollback_upload_file(task_id, ip, project_id, stack, cluster_name):
    """
    The function is used to restore the upload file to its initial status after upload or restart operations are done
    :param task_id:
    :param ip: The IP of the server to run the commands on
    :param project_id:
    :param stack: kafka/aerospike/elastic
    :param cluster_name:
    :return:
    """
    try:
        # Let's restore the upload file
        logging.info("Creating the original upload script")
        command = "sudo echo 'put your upload script here' > /var/tmp/upload.sh"
        logging.info('Going to run the command ' + command)
        create_output, create_exit_code = remote_ssh_command_execution(task_id, ip, command)
        logging.info(create_output)
        logging.info(create_exit_code)

        logging.info("Overwriting upload script")
        command = "gsutil cp /var/tmp/upload.sh gs://data-" + project_id + "/upload_scripts/" + stack + "/" \
                  + cluster_name + "/upload.sh"
        logging.info('Going to run the command ' + command)
        overwrite_output, overwrite_exit_code = remote_ssh_command_execution(task_id, ip, command)
        logging.info(overwrite_output)
        logging.info(overwrite_exit_code)
    except Exception as e:
        logging.error('error: ' + str(e))


def selective_cluster_dict(full_dict, include_list, task_id):
    """
    get the result dict with the entire cluster structure and based on provided list
    create a new dict with only selected nodes.
    :param task_id:
    :param full_dict:
    :param include_list:
    :return:
    """
    try:
        new_dict = {}
        include_list = include_list.replace(" ", "")
        include_list = include_list.split(",")
        logging.info('Formatted include_list: ' + str(include_list))
        logging.debug(type(include_list))
        logging.info('full_dict: ' + str(full_dict))
        logging.info('include_list: ' + str(include_list))
        for key, value in full_dict.items():
            if str(key) in include_list:
                new_dict[key] = value
        if new_dict:
            logging.info('This is the new filtered result_dict: ' + str(new_dict))
            return new_dict
        else:
            status = "The new filtered dict is empty, aborting."
            logging.error(status)
            logging.error("filtered_list: " + str(include_list))
            update_task(str(task_id), 'Finished', 500, str(status), str(status))
            sys.exit(2)
    except Exception as e:
        logging.error('error: ' + str(e))
        update_task(str(task_id), 'Finished', 500, str(e)[:65535], str(e))
        sys.exit(2)


@retry(stop=(stop_after_attempt(10)))
def instance_group_action_verify(task_id, instance_group, zone):
    """
    Verify that the instance group create command was success and that the instance group spinning
    :param task_id:
    :param instance_group:
    :param zone: the GCP zone where the instance group resides
    :return: 1 means success, 0 means no new instance is being creating
    """
    try:
        logging.debug('Let`s verify that a new instance is being created')
        cfg_results = getdefaultvalues()
        gcloud_path = cfg_results.get('gcloud_path')
        output = 0
        retry_counter = 0
        while output != 1:
            command = gcloud_path + ' compute instance-groups managed describe ' + instance_group + ' --zone=' \
                       + zone + ' |grep \' creating: 1\'|wc -l'
            logging.debug('About to execute: ' + str(command))
            process = subprocess.run(command, capture_output=True, text=True, shell=True)
            output = process.stdout
            output = int(output.strip())
            if output:
                logging.debug('output: ' + str(output))
            if output != 1:
                logging.debug('Could not find an instance in create status - waiting for 10 seconds then retry.')
                retry_counter += 1
                logging.debug('retry attempt: ' + str(retry_counter))
                sleep(10)
                if retry_counter >= 10:
                    logging.debug('retry counter breach (20 times * 10 secs sleep), let`s try and send replace command')
                    return 0
                continue
            output_error = process.stderr
            if process.returncode == 0:
                return output
            else:
                logging.error(output_error)
                raise 0
    except Exception as e:
        logging.error(str(e))
        update_task(str(task_id), 'Retry', 500, str(e)[:65535])


@retry(stop=(stop_after_attempt(10)))
def instance_group_resize(task_id, instance_group, zone, target_size):
    """
    Execute resize operation on target instance group
    :param task_id:
    :param instance_group:
    :param zone: the GCP zone where the instance group resides
    :param target_size: the new size of the target instance group
    :return:
    """
    try:
        cfg_results = getdefaultvalues()
        gcloud_path = cfg_results.get('gcloud_path')
        command = gcloud_path + ' compute instance-groups managed resize --size=' + str(target_size) + ' ' +\
                  instance_group + ' --zone=' + zone
        logging.debug('About to execute: ' + str(command))
        process = subprocess.run(command, capture_output=True, text=True, shell=True)
        output = process.stdout
        if output:
            logging.debug('output: ' + str(output))
        output_error = process.stderr
        if process.returncode == 0:
            return output
        else:
            raise Exception(output_error)
    except Exception as e:
        logging.error(str(e))
        update_task(str(task_id), 'Retry', 500, str(e)[:65535])

