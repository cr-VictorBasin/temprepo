import hashlib
import logging

from di_app_manager.app_code.kafka_rolling_api import *


def execute_aerospike_rolling_action(task_id, payload=None, results_dict=None, stack="aerospike"):
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
    status = 'status: '
    asadm_ip = None
    first_instance_name = None

    try:
        # Verify operation is valid (replace only)
        action = action.lower()
        if action not in ['replace', 'restart', 'upload']:
            status = 'action should be only replace/restart or upload'
            logging.error('The selected action is not valid, only action replace/restart or upload is valid')
            status_code = 202
            update_task(str(task_id), 'Finished', str(status_code), str(status), str(results_dict))
            sys.exit(1)

        # Get Aerospike cluster size
        cluster_size = 0
        logging.info('Checking cluster size')
        count = 0
        for key, value in full_results_dict.items():
            if key != -9999:
                cluster_size += 1
                if count == 0:
                    first_instance_name = get_instance_from_instance_group(task_id, value['instance group'],
                                                                           value['zone'], project_id)
                    logging.info("Got the first instance name for future commands: " + str(first_instance_name))
                count += 1

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

                    # Let's make sure target node is up and running before moving forward
                    logging.info('Lets check if port 22 for ssh is open at aerospike node')
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

                    # Settings migration delay to 0 in order to allow partitions to move immediately
                    ignore_migrate_fill_delay(0, task_id, ip_address)

                    # Speed up migration parameters
                    speed_migration_parameters(task_id, ip_address, 'enable', cluster_name)

                    # Checking cluster status
                    aerospike_cluster_stable_check(task_id, ip_address, cluster_size, cluster_name, target_mail_address)

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

                    if action == 'replace':
                        # quiesce the node
                        logging.info("quiesce node: " + str(instance_name))
                        command = "asinfo -v \'quiesce:\'"
                        quiesce_output, quiesce_exit_code = remote_ssh_command_execution(task_id, ip_address, command)
                        logging.debug(quiesce_output)
                        logging.debug(quiesce_exit_code)

                        # running recluster
                        logging.info("Running re-cluster")
                        command = "asadm --enable -e \'manage recluster\'"
                        recluster_output, recluster_exit_code = \
                            remote_ssh_command_execution(task_id, ip_address, command)
                        logging.debug(recluster_output)
                        logging.debug(recluster_exit_code)

                        # Checking that quiesce node operation succeeded, exiting if not
                        error = 0
                        if quiesce_exit_code != 0 or recluster_exit_code != 0:
                            if quiesce_exit_code != 0:
                                error = ('The quiesce command failed with exit code: ' + str(quiesce_exit_code)
                                         + ' exiting...')
                            elif recluster_exit_code != 0:
                                error = ('The quiesce command failed with exit code: ' + str(recluster_exit_code)
                                         + ' exiting...')

                            logging.error(error)
                            message = "Rolling " + action + " of " + cluster_name + " Failed with error: " \
                                      + str(error) + " " + str(results_dict)
                            subject = stack + " cluster: " + cluster_name + " Rolling " + action + " Failed"
                            sendemail(target_mail_address, {"subject": subject, "body": message})
                            update_task(str(task_id), 'Finished', 500, str(error)[:65535], str(results_dict))
                            sys.exit(1)

                        # checking cluster status
                        aerospike_cluster_stable_check(task_id, ip_address,
                                                       cluster_size, cluster_name, target_mail_address)

                    if action == 'restart' or action == 'replace':
                        command = "sudo service " + stack + " stop"
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
                        command = "sudo service " + stack + " start"
                        start_output, start_exit_code = remote_ssh_command_execution(task_id, ip_address, command)
                        status = status + ('start service exit_code:'
                                           ' ' + str(start_exit_code) + '\n' + str(start_output))
                        logging.debug(status)

                    logging.info(status)
                    subject = "Aerospike instance/cluster: " + instance_name + '@' + cluster_name + \
                              " Rolling " + action + " is moving forward"
                    results_dict[key]['progress'] = 'done'
                    nodes_rolled += 1
                    # Prepare and send email info about operation progress
                    status = timing_status + status
                    sendemail(target_mail_address, {"subject": subject, "body": str(status)})
                    update_task(str(task_id), 'Running', 200, str(status), str(results_dict))
                    check_kill_status(task_id)
                else:
                    if results_dict[key] != -9999:
                        logging.info(value['instance group'] + ' previously handled, skipping')

        # Setting the IP to run asadm command after last node is replaced in order to set migration delay
        asadm_ip = get_ip_from_instance(task_id, first_instance_name)
        asadm_ip = asadm_ip.strip()
        logging.info("asadm_ip is: " + str(asadm_ip) + " for instance: " + str(first_instance_name))

        # checking cluster status after the last node was replaced
        aerospike_cluster_stable_check(task_id, asadm_ip, cluster_size, cluster_name, target_mail_address)

        if action == 'restart' or action == 'upload':
            # Let's restore the upload file
            rollback_upload_file(task_id, asadm_ip, project_id, stack, cluster_name)

        if status_code == 200:
            status = str(action) + ' of cluster ' + str(cluster_name) + ' finished successfully.'

            # Setting back ignore-migrate-fill-delay to false after aerospike rolling replace
            logging.info("Setting migrate-fill-delay back to 3600")
            ignore_migrate_fill_delay(3600, task_id, asadm_ip)

            # Speed up migration parameters (disable)
            speed_migration_parameters(task_id, asadm_ip, 'disable', cluster_name)

            message = "Rolling " + action + " of " + cluster_name + " finished successfully " +\
                      format_dict_for_email(results_dict)
            logging.info(message)
            subject = stack + " cluster: " + cluster_name + " Rolling " + action + " finished successfully"
            sendemail(target_mail_address, {"subject": subject, "body": message})

        update_task(str(task_id), 'Finished', status_code, str(status)[:65535], str(results_dict))

    except Exception as e:

        logging.info("Setting ignore-migrate-fill-delay back to false")
        ignore_migrate_fill_delay(3600, task_id, asadm_ip)
        logging.error('error: ' + str(e))

        message = "Task ID: " + str(task_id) + "Rolling " + action + " of " + cluster_name + " Failed with error " \
                  + str(e) + " " + str(results_dict)
        logging.info(message)
        subject = stack + " cluster: " + cluster_name + " Rolling " + action + " Failed"
        sendemail(target_mail_address, {"subject": subject, "body": message})

        update_task(str(task_id), 'Finished', 500, str(e)[:65535], str(results_dict))


def aerospike_cluster_stable_check(task_id, ip_address, cluster_size, cluster_name, target_mail_address):
    """
    The function will run asadm command to check the cluster status and will stay in a loop until the cluster is stable
    and there are no remaining partitions migrations.
    :param task_id:
    :param ip_address:
    :param cluster_size: The cluster size
    :param cluster_name:
    :param target_mail_address:
    :return:
    """
    # Checking that cluster is stable
    try:
        logging.info('Checking cluster status')
        command = "asadm --enable -e  \"asinfo -v \'cluster-stable:size=" + str(cluster_size) +\
                  ";ignore-migrations=no\'\" | grep ERROR | head -1 | awk -F '::' '{print $2}'"

        output, exit_code = remote_ssh_command_execution(task_id, ip_address, command)

        elapsed = 0
        while len(output) != 0:
            logging.info('sleeping for 120 seconds before re-checking cluster status')
            check_kill_status(task_id)
            time.sleep(120)
            elapsed += 120
            ignore_migrate_fill_delay(0, task_id, ip_address)
            output, exit_code = remote_ssh_command_execution(task_id, ip_address, command)

            elapsed_check = elapsed % 3600

            if elapsed_check == 0:
                subject = "Aerospike cluster: " + str(cluster_name) + " cluster is not stable"
                body = "Aerospike cluster status check is running too long, check manually why it is stuck, " \
                       "run the command: " + command
                sendemail(target_mail_address, {"subject": subject, "body": body})

        # Checking that there are no migrations

        logging.info("Checking migration status")
        status = check_migrations(task_id, ip_address)
        logging.info("check_migration_status is: " + str(status))

        elapsed = 0
        while status == 10:
            logging.info('sleeping for 60 seconds before re-checking cluster migrations')
            check_kill_status(task_id)
            time.sleep(60)
            elapsed += 60
            status = check_migrations(task_id, ip_address)
            logging.info(status)

            elapsed_check = elapsed % 3600

            if elapsed_check == 0:
                subject = "Aerospike cluster: " + str(cluster_name) + " cluster partitions migrations still in progress"
                body = "Aerospike partitions migrations check is running too long, check manually why it is stuck"
                sendemail(target_mail_address, {"subject": subject, "body": body})

    except Exception as message:
        logging.error('error: ' + str(message))
        return 1, str(message)


def check_migrations(task_id, ip_address):
    """
    Function to check aerospike partitions migration progress.
    :param task_id:
    :param ip_address:
    :return: 0 - migration is completed, 10 - migration still in progress, 1 - operation failed
    """
    try:
        command = "asadm -e \'show stat like remain\' --json 2</dev/null | sed 1,2d"

        data_exit_code, data_disk, data_huge = show_stat_asadm_to_json(task_id, ip_address, command)

        if data_exit_code != 0:
            message = data_disk
            logging.debug(message)
            return 1, str(message)

        # Iterating json to catch pending migrations
        logging.info("checking disk based namespace")
        for key in data_disk:
            appeals_tx = key['appeals_tx_remaining']['raw']
            migrate_rx = key['migrate_rx_partitions_remaining']['raw']
            migrate_signals = key['migrate_signals_remaining']['raw']
            migrate_tx_partitions_lead = key['migrate_tx_partitions_lead_remaining']['raw']
            migrate_tx_partitions = key['migrate_tx_partitions_remaining']['raw']
            if appeals_tx != 0 or migrate_rx != 0 or migrate_signals != 0 or migrate_tx_partitions_lead != 0 \
                    or migrate_tx_partitions != 0:
                return 10

        logging.info("Checking memory based namespace")
        for key in data_huge:
            appeals_tx = key['appeals_tx_remaining']['raw']
            migrate_rx = key['migrate_rx_partitions_remaining']['raw']
            migrate_signals = key['migrate_signals_remaining']['raw']
            migrate_tx_partitions_lead = key['migrate_tx_partitions_lead_remaining']['raw']
            migrate_tx_partitions = key['migrate_tx_partitions_remaining']['raw']
            if appeals_tx != 0 or migrate_rx != 0 or migrate_signals != 0 or migrate_tx_partitions_lead != 0 \
                    or migrate_tx_partitions != 0:
                return 10
        return 0

    except Exception as message:
        logging.error('error: ' + str(message))
        return 1, str(message)


def show_stat_asadm_to_json(task_id, ip_address, command):
    """
    Will get asadm show stat like remain command and will return results in json
    :param task_id:
    :param ip_address:
    :param command: The asadm command
    :return: info_huge = the json with the huge db remaining migration,
    info_disk = the json with disk db remaining migrations
    """
    try:
        result, exit_code = remote_ssh_command_execution(task_id, ip_address, command)

        # Taking the result of the info asadm command and saving it to a file
        file = "output"
        with open(file, "w") as text_file:
            print(result, file=text_file)
        stdout = open(file, 'r')
        lines = stdout.readlines()
        output = ''

        for line in lines:
            output += line.strip()

        # Finding the last occurance of title to get the huge namespace data
        message = output.rfind("title")

        # Taking the substring from the last title occurrence till the end
        data_huge = output[message-2:]

        # Removing the huge data from the output and finding the disk based data
        output = output.replace(data_huge, '')
        message = output.rfind("title")
        data_disk = output[message-2:]

        data_huge = json.loads(data_huge)
        data_disk = json.loads(data_disk)

        info_huge = data_huge['groups'][0]['records']
        info_disk = data_disk['groups'][0]['records']

        return 0, info_disk, info_huge

    except Exception as message:
        logging.error('Failed to create the json from the asadm command: ' + str(message))
        return 1, str(message)


def ignore_migrate_fill_delay(config, task_id, ip_address):
    """
    Set partitions migration delay to 0 or 3600
    :param config: 0/3600-> 0 will cancel the delay and partitions will migrate without delay, 3600 will set the delay
    migration according to the default configuration
    :param task_id:
    :param ip_address:
    :return: 0 if config was changed successfully, 1 if it config change failed
    """
    try:
        command = "asadm --enable -e \'manage config service param migrate-fill-delay to " + str(config) + "\'"
        remote_ssh_command_execution(task_id, ip_address, command)

        message = "Successfully changed ignore-migrate-fill-delay to " + str(config)
        logging.info(message)
        return 0, str(message)

    except Exception as message:
        logging.error('Failed to configure ignore-migrate-fill-delay with error: ' + str(message))
        return 1, str(message)


def speed_migration_parameters(task_id, ip_address, status, cluster_name):
    """

    :param ip_address:
    :param task_id:
    :param status: enable to speed up, disable to revert to default values.
    :param cluster_name:
    :return:
    """
    try:

        if status == 'enable':
            migrate_sleep = 0
            migrate_threads = 20
            migrate_max_num_incoming = 80
        else:
            migrate_sleep = 1
            migrate_threads = 2
            migrate_max_num_incoming = 8

        command = "asadm --enable -e \"asinfo -v \'set-config:context=namespace;id=" + str(cluster_name) +\
                  ";migrate-sleep=" + str(migrate_sleep) + "\'\""
        remote_ssh_command_execution(task_id, ip_address, command)

        command = "asadm --enable -e \"asinfo -v \'set-config:context=service;migrate-threads=" \
                  + str(migrate_threads) + "\'\""
        remote_ssh_command_execution(task_id, ip_address, command)

        command = "asadm --enable -e \"asinfo -v \'set-config:context=service;migrate-max-num-incoming="\
                  + str(migrate_max_num_incoming) + "\'\""
        remote_ssh_command_execution(task_id, ip_address, command)

        message = "Successfully " + status + " speed up migration parameters"
        logging.info(message)
        return 0, str(message)

    except Exception as message:
        logging.error('Failed to configure ignore-migrate-fill-delay with error: ' + str(message))
        return 1, str(message)
