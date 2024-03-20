import logging

from di_app_manager.app_code.cluster_deployment import LogDeployment
from di_app_manager.app_code.git_procs import *
from di_app_manager.app_code.kafka_rolling_api import *
from di_app_manager.app_code.elastic_rolling_api import *
from di_app_manager.app_code.validation import *
from di_app_manager.app_code.aerospike_rolling_api import *
import math
import random


def scale_up_cluster(task_id, payload, cluster_stack):
    """
    Get the request payload, parse the info and scale the cluster accordingly
    :param cluster_stack:
    :param task_id:
    :param payload:
    :return: statue code and return msg (error or success)
    """
    
    logging.info('This is my task_id: ' + str(task_id))
    target_mail_address = GetKeyValueFromDict(payload.data, 'target_mail_address')
    project_id = GetKeyValueFromDict(payload.data, 'project_id')
    project_name = get_project_name_from_id(project_id)
    cluster_name = GetKeyValueFromDict(payload.data, 'cluster_name')
    target_size = GetKeyValueFromDict(payload.data, 'target_size')
    force_terraform_apply = GetKeyValueFromDict(payload.data, 'force_terraform_apply')

    try:
        # Let's check that the new target size is valid and is a multiple of zone count
        logging.debug("git path to use is: " + str(cluster_stack + '_git_path'))
        gitcheckout_exit_code, temp_folder = gitcheckout('master', cluster_stack + '_git_path')
        project_dir = os.path.join(temp_folder, 'projects', project_name, cluster_name)
        status, old_value, zone_count, line_number = get_current_cluster_info(cluster_stack, project_dir)
        logging.debug('Target_size: ' + str(target_size))
        logging.debug('old_value (normalized with zone_count): ' + str(old_value))
        logging.debug('zone_count: ' + str(zone_count))
        if status != 0:
            raise Exception(old_value)
        if int(target_size) <= 0:
            raise Exception('target size must be positive number')
        if int(target_size) % int(zone_count) != 0:
            raise Exception('The new target size: ' + (str(target_size) +
                                                       ' Must be a multiple of zone_count: ' + str(zone_count)))
        if (int(target_size) / int(zone_count)) <= int(old_value):
            raise Exception('target size: ' + str(target_size) + ' must be larger then current size: '
                            + str(int(old_value) * int(zone_count)))

        scale_cluster_shell(task_id, target_mail_address, project_id, cluster_name, target_size,
                            force_terraform_apply, cluster_stack)

    except Exception as e:
        logging.error('error: ' + str(e))
        update_task(task_id, 'Finished', 500, 'Internal Error: ' + str(e))


def scale_cluster_shell(task_id, target_mail_address, project_id, cluster_name, target_size,
                        force_terraform_apply, cluster_stack):
    """

    :param task_id:
    :param target_mail_address:
    :param project_id:
    :param cluster_name:
    :param target_size:
    :param force_terraform_apply:
    :param cluster_stack:
    :return:
    """
    status = None
    status_code = None
    try:
        logging.info('This is my task_id: ' + str(task_id))
        project_name = get_project_name_from_id(project_id)

        # Validate that target_size is a positive number
        if int(target_size) <= 0:
            status = 'target cluster size must be a positive number.'
            logging.error(status)
            update_task(task_id, 'Finished', 202, status)
            return None

        # Validate if force_terraform_apply false/true
        if force_terraform_apply.lower() not in ('true', 'false'):
            status = 'force_terraform_apply should be true ot false only.'
            logging.error(status)
            update_task(task_id, 'Finished', 202, status)
            return None

        # Validate if project exists
        return_code, return_msg = check_project_exists(project_id)
        if return_code == 0 and return_msg == 'not exists':
            status = 'GCP Project: ' + str(project_id) + ' does not exists'
            logging.error(status)
            update_task(task_id, 'Finished', 202, status)
            return None

        check_kill_status(task_id)
        gitcheckout_exit_code, temp_folder = gitcheckout('master', cluster_stack + '_git_path')
        project_dir = os.path.join(temp_folder, 'projects', project_name, cluster_name)

        # Check that the cluster directory exists, ie that the cluster exists
        if not os.path.isdir(project_dir):
            status = ('Cluster: ' + str(cluster_name) + ' does not exists or is not managed by DI manager')
            logging.error(status)
            update_task(task_id, 'Finished', 202, status)
            return None

        if gitcheckout_exit_code == 0:
            terraform_action(project_dir, 'init')
            plan_exit_code, plan_output = terraform_action(project_dir, 'plan')
            logging.debug('plan_exit_code: ' + str(plan_exit_code))
            check_kill_status(task_id)
            if plan_exit_code == 0 or force_terraform_apply.lower() == 'true':
                logging.info('force_terraform_apply is set to true, any Terraform state changes will be applied.')
                update_var_exit_code, update_var_msg, new_cluster_size = \
                    update_var_for_scale_up(cluster_stack, target_size, project_dir, force_terraform_apply)
                if update_var_exit_code != 0:
                    status = update_var_msg
                    status_code = 400
                elif update_var_exit_code == 0:
                    check_kill_status(task_id)
                    apply_exit_code, apply_output = terraform_action(project_dir, 'apply')
                    if apply_exit_code == 0:
                        status = cluster_stack + ' Cluster: ' + cluster_name + ' successfully Scaled, cluster size: ' \
                                 + str(new_cluster_size) + \
                                 '\n Note that cluster partitions migrations is running and might take time until ' \
                                 'the new nodes will be fully added service wise'
                        check_kill_status(task_id)
                        gitcommitpush_exit_code, gitcommitpush_status = gitcommitpush(project_dir, status)
                        if gitcommitpush_exit_code == 0:
                            status_code = 200
                        else:
                            status = 'Task ID: ' + str(task_id) + ' Unable to git commit and push (Cluster was' \
                                                                  ' scaled), error: ' + str(gitcommitpush_status)
                            status_code = 201
                    else:
                        status = 'Task ID: ' + str(task_id) + ' Failed to apply Scaled ' \
                                 + cluster_stack + ' cluster operation'
                        status_code = 500
            else:
                status = 'Terraform plan returned discrepancies, please exit and fix or Approve and proceed'
                status_code = 202
        else:
            status = 'Task ID: ' + str(task_id) + ' Unable to Checkout git project'
            status_code = 500
        mail_dict = {"subject": 'DI Manager: ' + str(status),
                     "body": str(project_id) + '@' + cluster_name + os.linesep + os.linesep + "Status: " + str(status)}
        LogRequest('scale_cluster', str(project_id) + '@' + cluster_name, status_code, status, 'post')
        LogDeployment(str(project_id) + '@' + cluster_name, 'online', cluster_stack)
        sendemail(target_mail_address, mail_dict)
        logging.info('status: ' + status)
        if status_code == 200:
            success_cleanup(temp_folder)
        update_task(task_id, 'Finished', status_code, status)
    except Exception as e:
        logging.error('error: ' + str(e))
        update_task(task_id, 'Finished', 500, 'Internal Error: ' + str(e))


def get_current_cluster_info(tech, current_dir):
    """

    :param tech:
    :param current_dir:
    :return:
    """
    try:
        cfg_results = getdefaultvalues()
        vars_file = cfg_results.get('var_file')
        vars_file_full_path = os.path.join(current_dir, vars_file)
        with open(vars_file_full_path, 'r') as f:
            lines = f.readlines()
            for i, line in enumerate(lines):
                if tech + '_per_zone_count' in line:
                    current_value = lines[i + 2].strip()
                    line_number = i + 2
                    if 'default' in current_value:
                        current_value = (current_value.partition('=')[2]).strip()
                        logging.debug('current_value: ' + str(current_value))
                        logging.debug('line_number: ' + str(line_number))
                    else:
                        return 1, 'Unable to locate the ' + tech + '_per_zone_count Terraform var value', None, None

                elif 'instances_zone_list' in line:
                    instances_zone_list = lines[i + 3].strip()
                    if 'default' in instances_zone_list:
                        zone_list_string = (instances_zone_list.partition('=')[2]).strip()
                        zone_list = zone_list_string.strip('][ ').split(',')
                        zone_count = len(zone_list)
        return 0, current_value, zone_count, line_number
    except Exception as e:
        logging.error('error: ' + str(e))
        return 1, str(e), None, None


def update_var_for_scale_up(tech, new_value, current_dir, force_terraform_apply):
    logging.debug(tech)
    logging.debug(new_value)
    logging.debug(current_dir)
    """
    :param tech: The Technology we want to scale, ie: aerospike, elasticsearch-data, kafka etc
    :param new_value: The new scale target number, must be bigger then current number
    :param current_dir: The current working directory for the Terraform code
    :return:
    """

    try:
        cfg_results = getdefaultvalues()
        vars_file = cfg_results.get('var_file')
        vars_file_full_path = os.path.join(current_dir, vars_file)
        status, old_value, zone_count, line_number = get_current_cluster_info(tech, current_dir)
        if status != 0:
            return 1, old_value, None
        # Calculated real cluster size, target_size/how many zones - rounded up.
        new_cluster_size = math.ceil(int(new_value) / zone_count)
        real_cluster_size = (new_cluster_size * zone_count)
        current_cluster_size = int(old_value) * zone_count
        logging.debug('New real cluster target size: ' + str(real_cluster_size))
        logging.debug('current_cluster_size' + str(current_cluster_size))

        def replace_line(file_name, line_num, text):
            lines = open(file_name, 'r').readlines()
            lines[line_num] = text
            out = open(file_name, 'w')
            out.writelines(lines)
            out.close()

        if (int(old_value) >= int(new_cluster_size)) and force_terraform_apply.lower() == 'false':
            logging.error('The current cluster size: ' + str(old_value) +
                          ' should be bigger or equal to the new one: ' + str(new_cluster_size))
            return 1, 'The current cluster size: ' + str(
                old_value) + ' should be bigger or equal to the new one: ' + str(new_cluster_size), None

        replace_line(vars_file_full_path, line_number, 'default     = ' + str(new_cluster_size) + '\n')
        logging.info(
            'Successfully changed ' + tech + ' var value from: ' + str(old_value) + ' to: ' + str(new_cluster_size))
        return 0, None, real_cluster_size
    except Exception as e:
        logging.error('error: ' + str(e))
        return 1, str(e), None


def execute_kafka_scale_down(task_id, payload):
    """
    Accept payload with project_id, cluster_name, action and target_mail_address, then perform rolling restart/replace
     for the entire cluster
    :param task_id:
    :param payload: None if results_dict is provided and this is a continuation run
    :return:
    """
    
    status = None
    project_name = None
    cluster_name = None
    status_code = 200
    target_mail_address = None
    results_dict = None
    try:
        logging.info('This is my task_id: ' + str(task_id))
        cfg_results = getdefaultvalues()
        kafka_port = cfg_results.get('kafka_port')
        logging.debug('payload: ' + str(payload.data))
        project_id = GetKeyValueFromDict(payload.data, 'project_id')
        logging.debug('project_id: ' + project_id)
        project_name = get_project_name_from_id(project_id)
        target_mail_address = GetKeyValueFromDict(payload.data, 'target_mail_address')
        cluster_name = GetKeyValueFromDict(payload.data, 'cluster_name')
        partitions_throttling = GetKeyValueFromDict(payload.data, 'partitions_throttling')
        target_size = GetKeyValueFromDict(payload.data, 'target_size')
        results_dict = cluster_instance_groups_dict(task_id, project_id, cluster_name, 'kafka_')
        logging.info(results_dict)

        # Validate if project already exists
        return_code, return_msg = check_project_exists(project_id)
        if return_code == 0 and return_msg == 'not exists':
            status = 'GCP Project: ' + str(project_id) + ' does not exists'
            update_task(str(task_id), 'Finished', 202, str(status), None)

        # Make sure that partitions_throttling is positive number (larger than 1)
        partitions_throttling = int(partitions_throttling)
        if partitions_throttling < 1:
            status = 'partitions_throttling must be a positive number (larger than 1)'
            update_task(str(task_id), 'Finished', 202, str(status), str(results_dict))

        # Let's check that the new target size is valid and is a multiple of zone count
        gitcheckout_exit_code, temp_folder = gitcheckout('master', 'kafka_git_path')
        project_dir = os.path.join(temp_folder, 'projects', project_name, cluster_name)
        status, old_value, zone_count, line_number = get_current_cluster_info('kafka', project_dir)
        logging.debug('Target_size: ' + str(target_size))
        logging.debug('old_value (normalized with zone_count): ' + str(old_value))
        logging.debug('zone_count: ' + str(zone_count))
        if status != 0:
            raise Exception(old_value)
        if int(target_size) <= 0:
            raise Exception('target size must be positive number')
        if int(target_size) % int(zone_count) != 0:
            raise Exception('The new target size: ' + (str(target_size) +
                            ' Must be a multiple of zone_count: ' + str(zone_count)))
        if (int(target_size) / int(zone_count)) >= int(old_value):
            raise Exception('target size: ' + str(target_size) + ' must be smaller then current size: '
                            + str(int(old_value) * int(zone_count)))

        check_kill_status(task_id)
        # Before we start, lets disable healthcheck by changing the port to 22
        key, value = random.choice(list(results_dict.items()))
        ip_address = get_ip_from_instance_name(task_id, results_dict[key]['instance group'],
                                               results_dict[key]['zone'], project_id)
        change_health_check_port(task_id, ip_address, results_dict[key]['instance group'],
                                 project_id, cluster_name, '22')

        for key, value in sorted(results_dict.items(), reverse=True):
            if results_dict[key]['progress'] != 'done':
                check_kill_status(task_id)
                ip_address = get_ip_from_instance_name(task_id, value['instance group'], value['zone'], project_id)

                # Let's make sure the healthcheck is set to port 22
                change_health_check_port(task_id, ip_address, results_dict[key]['instance group'],
                                         project_id, cluster_name, '22')

                def count_broker_number():
                    exit_code = 1
                    output = None
                    while exit_code != 0:
                        output, exit_code = remote_ssh_command_execution(task_id, ip_address, 'kafkat brokers |wc -l')
                        output = int(output) - 1
                    return output

                logging.info('Allowing 60 seconds of sleep to let all the kafka nodes get updated')
                time.sleep(60)
                server_count = count_broker_number()
                logging.debug('Current server count: ' + str(server_count))
                logging.debug('Current target size: ' + str(target_size))
                logging.debug('if server_count equal to target_size then the resize is done')
                if int(server_count) <= int(target_size):
                    status = ('Cluster: ' + cluster_name + ' reached target size: ' + str(target_size))
                    update_task(str(task_id), 'Finished', status_code, str(status)[:65535], str(results_dict))
                    logging.info(status)
                    break

                def get_broker_id():
                    exit_code = 1
                    output = None
                    while exit_code != 0:
                        time.sleep(10)
                        output, exit_code = remote_ssh_command_execution(
                            task_id, ip_address,
                            'cat /kafka/config/server.properties |grep broker.id |cut -d "=" -f2| cut -f1 -d"]"')
                        check_kill_status(task_id)
                    output = output.strip()
                    return output

                broker_id = get_broker_id()
                logging.info('Lets handle broker: ' + str(broker_id))

                def decommission_broker_id(broker_id):
                    command = '/usr/bin/kf-tool/kf.sh drain ' + broker_id + ' ' + str(partitions_throttling)
                    count_command = "/usr/local/bin/kafka-cluster-manager --cluster-type generic --cluster-name" \
                                    " this_cluster stats|sed -n '/ Partition Count/,/^$/p' |" \
                                    " awk '/^" + broker_id + " /' | awk '{print $3}'"
                    count_output = '1'
                    count_exit_code = '1'
                    while str(count_output) != '0' and str(count_exit_code) != '0':
                        check_kill_status(task_id)
                        time.sleep(10)
                        logging.debug('About to execute: ' + str(count_command))
                        count_output, count_exit_code = remote_ssh_command_execution(task_id, ip_address, count_command)
                        if str(count_output) == '0' and str(count_exit_code) == '0':
                            logging.info('Found ' + str(count_output) + ' partitions at broker ' + str(broker_id))
                            break
                        logging.debug('About to execute: ' + str(command))
                        output, exit_code = remote_ssh_command_execution(task_id, ip_address, command)
                        if output:
                            logging.debug('output: ' + str(output))
                        else:
                            logging.debug('No output')

                server_count = count_broker_number()
                logging.info('Server count: ' + str(server_count))
                decommission_broker_id(broker_id)
                stop_exit_code = 1
                while int(stop_exit_code) != 0:
                    stop_output, stop_exit_code = remote_ssh_command_execution(task_id, ip_address,
                                                                               'sudo systemctl disable --now kafka')
                    # to be safe we mask the service
                    remote_ssh_command_execution(task_id, ip_address, 'sudo systemctl mask kafka')
                    logging.debug(stop_output)
                    logging.debug(stop_exit_code)

                logging.debug('New cluster size: ' + str(int(server_count)-1))

                # check if this is the time to run terraform. The time is when the current cluster size is a
                # multiple of zone_count.
                # We reduce 1 from server_count because server count was sampled before the node was stopped.
                check_kill_status(task_id)
                if (int(server_count)-1) % int(zone_count) == 0:
                    new_target_size = int(server_count) - int(zone_count)
                    # Fail-safe to make sure we don't scale more than we aimed to
                    if int(target_size) >= int(new_target_size):
                        logging.debug('new_target_size: ' + str(new_target_size))
                        logging.info('About to scale cluster: ' + cluster_name + ' at GCP project: ' + project_id +
                                     ' to ' + str(new_target_size))
                        scale_cluster_shell(task_id, target_mail_address, project_id, cluster_name, new_target_size,
                                            'true', 'kafka')

                # sendemail(target_mail_address, {status: ''})
                check_kill_status(task_id)
                results_dict[key]['progress'] = 'done'
                status = 'Finished removing broker: ' + str(broker_id)
                update_task(str(task_id), 'Running', 200, str(status), str(results_dict))

        # let's re-enable healthcheck by changing the original port
        key, value = random.choice(list(results_dict.items()))
        ip_address = get_ip_from_instance_name(task_id, results_dict[key]['instance group'],
                                               results_dict[key]['zone'], project_id)
        change_health_check_port(task_id, ip_address, results_dict[key]['instance group'], project_id,
                                 cluster_name, kafka_port)
        status = 'DI Manager: Kafka cluster: ' + str(cluster_name) + \
                 ' Finished scale down operation, new cluster size: ' + str(target_size)
        update_task(str(task_id), 'Finished', 200, str(status), str(results_dict))
        sendemail(target_mail_address, {"subject": status, "body": ' '})
    except Exception as e:
        logging.error('error: ' + str(e))

        message = 'Task ID: ' + str(task_id) + ' Kafka scale down operation ERROR for cluster: ' + str(cluster_name) + \
                  ' at GCP project: ' + str(project_name) + ' with error: ' + str(e)
        logging.info(message)
        sendemail(target_mail_address, {"subject": message, "body": str(results_dict)})

        update_task(str(task_id), 'Finished', 500, str(e)[:65535], str(results_dict))

def elastic_scale_down(task_id, payload):
    """
    elastic scale down API, gets payload with target cluster nodes and scales down to that number
    :param task_id:
    :param payload:
    :return:
    """

    status = None
    project_name = None
    cluster_name = None
    status_code = 200
    target_mail_address = None
    results_dict = None
    try:
        logging.info('This is my task_id: ' + str(task_id))
        cfg_results = getdefaultvalues()
        elastic_user = cfg_results.get('elastic_user')
        elastic_pass = cfg_results.get('elastic_pass')
        elastic_port = cfg_results.get('elastic_port')
        logging.debug('payload: ' + str(payload.data))
        project_id = GetKeyValueFromDict(payload.data, 'project_id')
        logging.debug('project_id: ' + project_id)
        project_name = get_project_name_from_id(project_id)
        target_mail_address = GetKeyValueFromDict(payload.data, 'target_mail_address')
        cluster_name = GetKeyValueFromDict(payload.data, 'cluster_name')
        target_size = GetKeyValueFromDict(payload.data, 'target_size')
        results_dict = cluster_instance_groups_dict(task_id, project_id, cluster_name, 'elastic_')
        logging.info(results_dict)
        masters_results_dict = cluster_instance_groups_dict(task_id, project_id, cluster_name, 'master_')
        logging.info(f"\033[1;33;40m masters_results_dict:\033[1;32;40m {masters_results_dict}")

        # Validate if project already exists
        return_code, return_msg = check_project_exists(project_id)
        if return_code == 0 and return_msg == 'not exists':
            status = 'GCP Project: ' + str(project_id) + ' does not exists'
            update_task(str(task_id), 'Finished', 202, str(status), None)

        # Let's check that the new target size is valid and is a multiple of zone count
        gitcheckout_exit_code, temp_folder = gitcheckout('master', 'elastic_git_path')
        project_dir = os.path.join(temp_folder, 'projects', project_name, cluster_name)
        status, old_value, zone_count, line_number = get_current_cluster_info('elastic', project_dir)
        logging.debug('Target_size: ' + str(target_size))
        logging.debug('old_value (normalized with zone_count): ' + str(old_value))
        logging.debug('zone_count: ' + str(zone_count))
        if status != 0:
            raise Exception(old_value)
        if int(target_size) <= 0:
            raise Exception('target size must be positive number')
        if int(target_size) % int(zone_count) != 0:
            raise Exception('The new target size: ' + (str(target_size) +
                                                       ' Must be a multiple of zone_count: ' + str(zone_count)))
        if (int(target_size) / int(zone_count)) >= int(old_value):
            raise Exception('target size: ' + str(target_size) + ' must be smaller then current size: '
                            + str(int(old_value) * int(zone_count)))

        # Get elastic cluster size
        cluster_size = 0
        logging.info('Checking cluster size')
        for key, value in results_dict.items():
            if cluster_size == 0:
                first_instance_name = get_instance_from_instance_group(task_id, value['instance group'],
                                                                       value['zone'], project_id)
                ip_address = get_ip_from_instance(task_id, first_instance_name)

            cluster_size += 1
        logging.info("Cluster size found is: " + str(cluster_size))

        # Get elastic master ip
        logging.info('Get elastic master ip')
        for key, value in masters_results_dict.items():
                first_instance_name = get_instance_from_instance_group(task_id, value['instance group'],
                                                                       value['zone'], project_id)
                master_ip_address = get_ip_from_instance(task_id, first_instance_name)
        logging.info("\033[1;33;40m elastic master ip:\033[1;32;40m " + str(master_ip_address))

        # Let's check elastic cluster status
        elastic_cluster_status = get_elastic_cluster_status(task_id, host=master_ip_address)
        if elastic_cluster_status != 'green': 
          raise Exception(f'the cluster status {elastic_cluster_status}, cluster is not stable, exit')
        
        logging.debug(f"cluster status {elastic_cluster_status}")

        # Number of nodes to remove
        nodes_rm_count = int(cluster_size) - int(target_size)
        logging.info("Going to remove " + str(nodes_rm_count) + " nodes from the cluster")

        nodes_rm_ips = []
        nodes_rm_ips_str = ''
        # Let's quest the nodes
        for key, value in sorted(results_dict.items(), reverse=True):
            #logging.debug(f"key: {key}, value: {value}")
            # from the instance group, get the ip address
            instance_name = get_instance_from_instance_group(task_id, value['instance group'],
                                                             value['zone'], project_id)
            ip_address = get_ip_from_instance(task_id, instance_name)
            logging.info(f"The instance_name is: \033[1;33;40m {instance_name}, ip_address is: {ip_address}\033[1;32;40m")
            nodes_rm_ips.append(ip_address) 
            nodes_rm_count -= 1
            if nodes_rm_count == 0:
                logging.debug(f"nodes_rm_ips list = {nodes_rm_ips}")
                break

        # allocation.exclude._ip for the nodes
        logging.info("allocation.exclude._ip run on node: " + str(instance_name))
        nodes_rm_ips_str = ', '.join(map(str, nodes_rm_ips))
        exclude_ips = "curl -s -k -u " + elastic_user + ":" + elastic_pass + \
                       " -XPUT \"https://127.0.0.1:" + str(elastic_port) + \
                       "/_cluster/settings\" -H " "'Content-Type: application/json' -d' {\"transient\":" \
                       ' { \"cluster.routing.allocation.exclude._ip\": \"' + nodes_rm_ips_str + '\"}}\''
        exclude_output, exclude_exit_code = remote_ssh_command_execution(task_id, master_ip_address, exclude_ips)
        #logging.debug(exclude_exit_code)
        logging.debug(exclude_output)


        # Let's check that there are no shards on the nodes and the cluster is stable
        logging.info('\033[1;33;40m*\033[1;32;40m' * 100)
        logging.info('Lets check that there are no shards on the nodes and the cluster is stable')

        shards_output = get_elastic_shards_status(task_id, hosts=nodes_rm_ips)
        if int(shards_output) == 0:
            logging.debug("\033[1;33;40m Checking the number of shards is completed, there is no data on all nodes to be deleted \033[1;32;40m")

        elastic_cluster_status = None
        elastic_cluster_status_not_green = None
        # Let's check elastic cluster status before removing nodes
        elastic_cluster_status = get_elastic_cluster_status(task_id, host=master_ip_address)
        if elastic_cluster_status != 'green': 
          elastic_cluster_status_not_green = 0
          while elastic_cluster_status != 'green': 
            elastic_cluster_status = get_elastic_cluster_status(task_id, host=master_ip_address)
            elastic_cluster_status_not_green += 1
            logging.debug(f"cluster status \033[1;33;40m {elastic_cluster_status}\033[1;32;40m {elastic_cluster_status_not_green}")
            check_kill_status(task_id)
            if elastic_cluster_status_not_green == 60:
              message = "Task ID: " + str(task_id) + " elastic_scale_down for " + cluster_name + " request for check the cluster manualy " + str(results_dict)
              subject = "elastic cluster status is not green: " + cluster_name + " elastic_scale_down request for check the cluster"
              sendemail(target_mail_address, {"subject": subject, "body": message})
            time.sleep(60)

        # Let's check that there are no shards on the nodes if 
        # cluster status was not green
        if elastic_cluster_status_not_green is not None:
          logging.info('\033[1;33;40mThe cluster status was not green, let\'s check the shards status again\033[1;32;40m')
          shards_output = get_elastic_shards_status(task_id, hosts=nodes_rm_ips)
          if int(shards_output) == 0:
              logging.debug("\033[1;33;40m second check number of shards is completed, there is no data on all nodes to be deleted \033[1;32;40m")

        logging.info('\033[1;33;40m*\033[1;32;40m' * 100)
        # Let's update the new server count
        scale_cluster_shell(task_id, target_mail_address, project_id, cluster_name, target_size, 'true', 'elastic')

        # set allocation.exclude._ip of the cluster back to null
        logging.info("\033[1;33;40m set allocation.exclude._ip \033[1;32;40m of the cluster back to null on node: " + str(master_ip_address))
        exclude_ips = "curl -s -k -u " + elastic_user + ":" + elastic_pass + \
                       " -XPUT \"https://127.0.0.1:" + str(elastic_port) + \
                       "/_cluster/settings\" -H " "'Content-Type: application/json' -d' {\"transient\":" \
                       ' { \"cluster.routing.allocation.exclude._ip\": null}}\''
        exclude_output, exclude_exit_code = remote_ssh_command_execution(task_id, master_ip_address, exclude_ips)

        status = 'DI Manager: elastic cluster: ' + str(cluster_name) + \
                 ' Finished scale down operation, new cluster size: ' + str(target_size)
        update_task(str(task_id), 'Finished', 200, str(status), str(results_dict))
        sendemail(target_mail_address, {"subject": status, "body": ' '})

    except Exception as e:
        logging.error('error: ' + str(e))

        message = 'Task ID: ' + str(task_id) + ' Elastic scale down operation ERROR for cluster: ' + str(cluster_name) + \
                  ' at GCP project: ' + str(project_name) + ' with error: ' + str(e)
        logging.info(message)
        sendemail(target_mail_address, {"subject": message, "body": str(results_dict)})

        update_task(str(task_id), 'Finished', 500, str(e)[:65535], str(results_dict))


def get_info_from_google(task_id, ip_address, project_id, execute_command):
    """
    execute a command on a server (by remote ssh)

    :param task_id:
    :param ip_address:
    :param project_id: the ip address of the node
    :param execute_command: the command to be executed
    :return: the value returned by the command.
    """
    try:
        logging.info('using google api Parse the health check name from the instance group')
        cfg_results = getdefaultvalues()
        gcloud_path = cfg_results.get('gcloud_path')
        value = None
        exit_code = 99
        counter = 0
        while exit_code != 0:
            command = gcloud_path + " auth activate-service-account --key-file=/root/gcp_key.json; " + \
                      gcloud_path + " config set project " + project_id + '; ' + execute_command
            logging.debug('executing: ' + str(command))
            value, exit_code = remote_ssh_command_execution(task_id, ip_address, command)
            logging.debug('exit_code: ' + str(exit_code))
            if exit_code != 0:
                logging.debug('sleeping 60 seconds before retry, attempt: ' + str(counter))
                check_kill_status(task_id)
                time.sleep(60)
                counter += 1
                # Abort after 10 failed attempts
                if counter > 10:
                    raise 'Unable to get info from google, failed at: ' + str(command)
        return value
    except Exception as e:
        logging.error('error: ' + str(e))
        # sendemail(target_mail_address, {status: ''})
        update_task(str(task_id), 'Finished', 500, str(e)[:65535], str(e))


@retry(stop=(stop_after_attempt(10)))
def change_health_check_port(task_id, ip_address, instance_group, project_id, cluster_name, target_port):
    """
    Target the port of a healthcheck (identified by instance group name), then change the target monitored port.

    :param task_id:
    :param ip_address:
    :param instance_group:
    :param project_id:
    :param cluster_name:
    :param target_port:  new target port to be monitored by health check
    :return:
    """
    try:
        logging.info('Connecting to: ' + str(ip_address) + '@' + instance_group + ' port will be: ' + str(target_port))
        health_check_name = 'kafka-autohealing-health-check-' + cluster_name
        logging.debug('health_check_name: ' + str(health_check_name))
        command = "gcloud compute health-checks describe " + health_check_name + \
                  " |grep 'port:' |awk '{print $2}'"
        logging.debug('command: ' + command)
        current_port = get_info_from_google(task_id, ip_address, project_id, command)
        logging.debug('current_port: ' + str(current_port))
        command = "gcloud compute health-checks update tcp " + \
                  health_check_name + " --port " + str(target_port)
        logging.debug('command: ' + command)
        remote_ssh_command_execution(task_id, ip_address, command)
        logging.info('Healthcheck Port is now: ' + str(target_port))

        return current_port
    except Exception as e:
        logging.error('error: ' + str(e))


@retry(stop=(stop_after_attempt(10)))
def get_ip_from_instance_name(task_id, instance_group, zone, project_id):
    """
    Get an instance group name and parse the IP address from the output (assume single VM per instance group)

    :param task_id:
    :param instance_group:
    :param zone:
    :param project_id:
    :return: returns the ip_address of the instance
    """
    try:
        instance_name = get_instance_from_instance_group(task_id, instance_group, zone, project_id)
        ip_address = get_ip_from_instance(task_id, instance_name)
        ip_address = ip_address.strip()
        return ip_address
    except Exception as e:
        logging.error('error: ' + str(e))

def aerospike_scale_down(task_id, payload):
    """
    Aerospike scale down API, gets payload with target cluster nodes and scales down to that number
    :param task_id:
    :param payload:
    :return:
    """

    status = None
    project_name = None
    cluster_name = None
    status_code = 200
    target_mail_address = None
    results_dict = None
    try:
        logging.info('This is my task_id: ' + str(task_id))
        cfg_results = getdefaultvalues()
        logging.debug('payload: ' + str(payload.data))
        project_id = GetKeyValueFromDict(payload.data, 'project_id')
        logging.debug('project_id: ' + project_id)
        project_name = get_project_name_from_id(project_id)
        target_mail_address = GetKeyValueFromDict(payload.data, 'target_mail_address')
        cluster_name = GetKeyValueFromDict(payload.data, 'cluster_name')
        target_size = GetKeyValueFromDict(payload.data, 'target_size')
        results_dict = cluster_instance_groups_dict(task_id, project_id, cluster_name, 'aerospike_')
        logging.info(results_dict)

        # Validate if project already exists
        return_code, return_msg = check_project_exists(project_id)
        if return_code == 0 and return_msg == 'not exists':
            status = 'GCP Project: ' + str(project_id) + ' does not exists'
            update_task(str(task_id), 'Finished', 202, str(status), None)

        # Let's check that the new target size is valid and is a multiple of zone count
        gitcheckout_exit_code, temp_folder = gitcheckout('master', 'aerospike_git_path')
        project_dir = os.path.join(temp_folder, 'projects', project_name, cluster_name)
        status, old_value, zone_count, line_number = get_current_cluster_info('aerospike', project_dir)
        logging.debug('Target_size: ' + str(target_size))
        logging.debug('old_value (normalized with zone_count): ' + str(old_value))
        logging.debug('zone_count: ' + str(zone_count))
        if status != 0:
            raise Exception(old_value)
        if int(target_size) <= 0:
            raise Exception('target size must be positive number')
        if int(target_size) % int(zone_count) != 0:
            raise Exception('The new target size: ' + (str(target_size) +
                                                       ' Must be a multiple of zone_count: ' + str(zone_count)))
        if (int(target_size) / int(zone_count)) >= int(old_value):
            raise Exception('target size: ' + str(target_size) + ' must be smaller then current size: '
                            + str(int(old_value) * int(zone_count)))

        # Get Aerospike cluster size
        cluster_size = 0
        logging.info('Checking cluster size')
        for key, value in sorted(results_dict.items(), reverse=True):
            if cluster_size == 0:
                first_instance_name = get_instance_from_instance_group(task_id, value['instance group'],
                                                                       value['zone'], project_id)
                ip_address = get_ip_from_instance(task_id, first_instance_name)
                logging.debug("First Instance IP: " + ip_address)

            cluster_size += 1
        logging.info("Cluster size found is: " + str(cluster_size))

        # Let's check aerospike cluster status
        aerospike_cluster_stable_check(task_id, ip_address, cluster_size, cluster_name, target_mail_address)

        # Number of nodes to remove
        nodes_rm_count = int(cluster_size) - int(target_size)
        logging.info("Going to remove " + str(nodes_rm_count) + " nodes from the cluster")

        # Let's quest the nodes
        for key, value in sorted(results_dict.items(), reverse=True):
            logging.debug("key: " + str(key))
            logging.debug("value: " + str(value))

            # from the instance group, get the ip address
            instance_name = get_instance_from_instance_group(task_id, value['instance group'],
                                                             value['zone'], project_id)
            ip_address = get_ip_from_instance(task_id, instance_name)
            logging.info("The instance_name is: " + str(instance_name))
            logging.info("The ip_address is: " + str(ip_address))

            # Quest the node
            logging.info("quiesce node: " + str(instance_name))
            command = "asinfo -v \'quiesce:\'"
            result = as_command(task_id, ip_address, command, instance_name)

            if result != 0:
                raise Exception("quiesce failed for node " + str(instance_name))

            nodes_rm_count -= 1

            if nodes_rm_count == 0:
                break

        # running recluster
        logging.info("Running re-cluster")
        command = "asadm --enable -e \'manage recluster\'"
        result = as_command(task_id, ip_address, command, instance_name)

        if result != 0:
            raise Exception("recluster failed on node " + str(instance_name))

        # Speed up migration parameters
        speed_migration_parameters(task_id, ip_address, 'enable', cluster_name)

        # Let's check that the nodes were quest and cluster is stable
        aerospike_cluster_stable_check(task_id, ip_address, cluster_size, cluster_name, target_mail_address)

        #let's verify that there is no data on the removed nodes
        elapsed = 0
        status = 1
        while status != 0:
            logging.info('sleeping for 60 seconds before re-checking nodes data')
            check_kill_status(task_id)
            time.sleep(60)
            elapsed += 60
            status = check_data_on_node(task_id, ip_address, instance_name, cluster_size, target_size, cluster_name)
            logging.info(status)

            elapsed_check = elapsed % 3600

            if elapsed_check == 0:
                subject = "Aerospike cluster: " + str(cluster_name) + " data on node check still in progress"
                body = "Aerospike data on node check is running too long, check manually why it is stuck"
                sendemail(target_mail_address, {"subject": subject, "body": body})

        # Speed up migration parameters (disable)
        speed_migration_parameters(task_id, ip_address, 'disable', cluster_name)

        # Mask and stop the service on the nodes that being removed
        nodes_rm_count = int(cluster_size) - int(target_size)
        for key, value in sorted(results_dict.items(), reverse=True):
            logging.debug("key: " + str(key))
            logging.debug("value: " + str(value))

            # from the instance group, get the ip address
            instance_name = get_instance_from_instance_group(task_id, value['instance group'],
                                                             value['zone'], project_id)
            ip_address = get_ip_from_instance(task_id, instance_name)
            logging.info("The instance_name is: " + str(instance_name))
            logging.info("The ip_address is: " + str(ip_address))

            # Stop the service on the node
            logging.info("Stopping the aerospike service on node : " + str(instance_name))
            command = "sudo systemctl stop aerospike"
            command_output, command_exit_code = remote_ssh_command_execution(task_id, ip_address, command)
            logging.debug(command_exit_code)
            logging.debug(command_output)

            #Mask the service on the node
            logging.info("Masking aerospike service on node : " + str(instance_name))
            command = "sudo systemctl mask aerospike"
            command_output, command_exit_code = remote_ssh_command_execution(task_id, ip_address, command)
            logging.debug(command_exit_code)
            logging.debug(command_output)

            nodes_rm_count -= 1

            if nodes_rm_count == 0:
                break


    # Let's update the new server count
        scale_cluster_shell(task_id, target_mail_address, project_id, cluster_name, target_size, 'true', 'aerospike')

        update_task(str(task_id), 'Finished', 200, str(status), str(results_dict))
    except Exception as e:
        logging.error('error: ' + str(e))

        message = 'Task ID: ' + str(task_id) + ' Aerospike scale down operation ERROR for cluster: ' + str(cluster_name) + \
                  ' at GCP project: ' + str(project_name) + ' with error: ' + str(e)
        logging.info(message)
        sendemail(target_mail_address, {"subject": message, "body": str(results_dict)})

        update_task(str(task_id), 'Finished', 500, str(e)[:65535], str(results_dict))


@retry(stop=(stop_after_attempt(10)))
def as_command(task_id, ip_address, command, instance_name):
    """

    :param task_id:
    :param ip_address:
    :param command:
    :param instance_name:
    :return:
    """
    try:
        logging.info("Running " + command + " on node " + str(instance_name))
        command_output, command_exit_code = remote_ssh_command_execution(task_id, ip_address, command)
        logging.debug(command_output)
        logging.debug(command_exit_code)

        retry = 5
        while command_exit_code != 0 and retry != 0:
            command_output, command_exit_code = remote_ssh_command_execution(task_id, ip_address, command)
            logging.debug(command_output)
            logging.debug(command_exit_code)
            time.sleep(5)
            retry -= 1

        if command_exit_code !=0:
            raise Exception(command + " failed for node " + str(instance_name))
        else:
            return 0

    except Exception as e:
        logging.error('error: ' + str(e))

@retry(stop=(stop_after_attempt(10)))
def check_data_on_node(task_id, ip_address, instance_name, cluster_size, target_size, cluster_name):
    """

    :param task_id: The general task id
    :param ip_address: node IP address to run the command on
    :param instance_name: The instance name
    :param cluster_size:
    :param target_size:
    :param cluster_name:
    :return: 0 if there is no data on the nodes, 1 if there is data
    """

    try:
        command = "asadm -e 'info namespace object' --json | tail -n +3"
        logging.info("Running " + command + " on node " + str(instance_name))

        command_output, command_exit_code = remote_ssh_command_execution(task_id, ip_address, command)
        logging.debug(command_exit_code)

        data = json.loads(command_output)
        node = data['groups'][0]['records']

        nodes_rm_count = int(cluster_size) - int(target_size)
        low_range = int(cluster_size) - int(nodes_rm_count)

        logging.info("Running checks on nodes: " + str(low_range) + " to " + str(cluster_size))

        for i in range(0, cluster_size):
            for j in range(low_range, cluster_size):
                check = "aerospike-cluster-" + str(cluster_name) + "-instance-" + str(j)
                machine = node[i]['Node']['raw']
                if check in machine:
                    master = node[i]['Objects']['Master']['raw']
                    replica = node[i]['Objects']['Prole']['raw']
                    logging.info("The node is: " + machine)
                    logging.info("Master and replica data on node: " + str(j) + " is " + str(master) + " " + str(replica))
                    if master or replica != 0:
                        return 1
        return 0
    except Exception as e:
        logging.error('error: ' + str(e))








