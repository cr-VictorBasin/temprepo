import os.path

from di_app_manager.app_code.elastic_rolling_api import *
from di_app_manager.app_code.aerospike_rolling_api import *
from di_app_manager.app_code.git_procs import *


def sanity_kafka_cluster(task_id, payload):
    """
    Perform a sanity check of a kafka cluster:
    For each node execute:
    Check if there are under-replicated partitions
    Check if the kafka port is open and responsive
    Check if the ssh port(22) is open and responsive
    Check if server.log contain ERRORS
    :param task_id:
    :param payload:
    """
    
    logging.info('This is my task_id: ' + str(task_id))
    test_results = None
    target_mail_address = None
    project_name = None
    cluster_name = None
    kafka_port = None
    test_status = 0
    try:
        project_id, project_name, target_mail_address, cluster_name, action, results_dict, full_results_dict = \
            rolling_action_parameters(task_id, payload, None, 'kafka')

        project_id, project_name, target_mail_address, cluster_name, action, zk_results_dict, full_results_dict = \
            rolling_action_parameters(task_id, payload, None, 'zookeeper')

        # Validate if project exists
        return_code, return_msg = check_project_exists(project_id)
        if return_code == 0 and return_msg == 'not exists':
            status = 'GCP Project: ' + str(project_id) + ' does not exists'
            logging.error(status)
            update_task(task_id, 'Finished', 202, status)
            return None

        check_kill_status(task_id)
        gitcheckout_exit_code, temp_folder = gitcheckout('master', 'kafka_git_path')
        project_dir = os.path.join(temp_folder, 'projects', project_name, cluster_name)
        # Check that the cluster directory exists, ie that the cluster exists
        if not os.path.isdir(project_dir):
            status = ('Cluster: ' + str(cluster_name) + ' does not exists or is not managed by DI manager')
            logging.error(status)
            update_task(task_id, 'Finished', 202, status)
            return None

        cfg_results = getdefaultvalues()
        zookeeper_port = cfg_results.get('zookeeper_port')

        test_results = {}
        logging.info('Now checking zookeeper')
        for key, value in sorted(zk_results_dict.items(), reverse=True):
            check_kill_status(task_id)
            logging.debug("key: " + str(key))
            logging.debug("value: " + str(value))
            if key != -9999:
                instance_name = get_instance_from_instance_group(task_id, value['instance group'], value['zone'],
                                                                 project_id)
                instance_name = instance_name.strip()
                ip_address = get_ip_from_instance(task_id, instance_name)
                ip_address = ip_address.strip()
                test_results[instance_name] = {}
                test_results[instance_name]['ip address'] = ip_address

                logging.info('Lets check if port 22 for ssh is open at zookeeper node')
                ssh_status = 0
                if port_check(ip_address, 22):
                    logging.info("ssh port: " + str(zookeeper_port) + " is open at host: " + str(ip_address))
                    test_results[instance_name]['ssh-port-status'] = None
                else:
                    logging.info("ssh port: " + str(zookeeper_port) + " is un-responsive at host: " + str(ip_address))
                    test_results[instance_name]['ssh-port-status'] = 'port: ' + str(zookeeper_port) + \
                                                                     ' is un-responsive'
                    test_status = 1
                    ssh_status = 1

                # Without ssh access, skipped the rest of the tests
                if ssh_status == 0:
                    logging.info("Lets check zookeeper status: zkServer.sh status ")
                    command = "/usr/bin/zookeeper/bin/zkServer.sh status"
                    sk_output, zk_exit_code = remote_ssh_command_execution(task_id, ip_address, command)

                    if str(zk_exit_code) == '0':
                        test_results[instance_name]['zk-service-status'] = None
                    else:
                        test_results[instance_name]['zk-service-status'] = 'zookeeper un-available'
                        test_status = 1

                    logging.info("Get the latest zookeeper log file ")
                    command = "ls /usr/bin/zookeeper/logs -Art | tail -1"
                    log_output, log_exit_code = remote_ssh_command_execution(task_id, ip_address, command)
                    log_output = log_output.strip()
                    log_full_path = os.path.join('/usr/bin/zookeeper/logs', log_output)
                    logging.info("Parsing this log file: " + str(log_full_path))
                    command = "cat " + log_full_path + " |grep 'ERR' |wc -l"
                    zk_err_output, zk_err_exit_code = remote_ssh_command_execution(task_id, ip_address, command)
                    zk_err_output = zk_err_output.strip()

                    if str(zk_err_output) == '0':
                        test_results[instance_name]['log-errors'] = None
                    else:
                        test_results[instance_name]['log-errors'] = 'Found: ' + str(zk_err_output) + \
                                                                    ' errors in ' + log_output
                        test_status = 1
                else:
                    test_results[instance_name]['zk-service-status'] = 'skipped'
                    test_results[instance_name]['log-errors'] = 'skipped'

        logging.info('Now checking Kafka')
        for key, value in sorted(results_dict.items(), reverse=True):
            check_kill_status(task_id)
            logging.debug("key: " + str(key))
            logging.debug("value: " + str(value))
            if key != -9999:
                instance_name = get_instance_from_instance_group(task_id, value['instance group'], value['zone'],
                                                                 project_id)
                instance_name = instance_name.strip()
                ip_address = get_ip_from_instance(task_id, instance_name)
                ip_address = ip_address.strip()
                test_results[instance_name] = {}
                test_results[instance_name]['ip address'] = ip_address

                logging.info('Lets check if port 22 for ssh is open')
                ssh_status = 0
                if port_check(ip_address, 22):
                    logging.info("ssh port: 22 is open at host: " + str(ip_address))
                    test_results[instance_name]['ssh-port-status'] = None
                else:
                    logging.info("ssh port: 22 is un-responsive at host: " + str(ip_address))
                    test_results[instance_name]['ssh-port-status'] = 'port: 22 is un-responsive'
                    test_status = 1
                    ssh_status = 1

                if ssh_status == 0:
                    logging.info("Lets check kafka port status")
                    logging.debug("Getting Kafka port from server.properties")
                    command = "cat /kafka/config/server.properties |grep advertised.listeners | tail -c 5"
                    port_output, exit_code = remote_ssh_command_execution(task_id, ip_address, command)

                    if port_output.isnumeric():
                        kafka_port = port_output
                    else:
                        kafka_port = 9093
                    logging.debug('Kafka is using port: ' + str(kafka_port))

                    if port_check(ip_address, kafka_port):
                        logging.info("kafka port: " + str(kafka_port) + " is open at host: " + str(ip_address))
                        test_results[instance_name]['kafka-port-status'] = None
                    else:
                        logging.info("kafka port: " + str(kafka_port) + " is un-responsive at host: " + str(ip_address))
                        test_results[instance_name]['kafka-port-status'] = 'port: ' + str(kafka_port) + \
                                                                           ' is un-responsive'
                        test_status = 1

                    logging.info("Checking kafka under-replicated partitions")
                    command = "kafkat partitions --under-replicated| wc -l"
                    output, exit_code = remote_ssh_command_execution(task_id, ip_address, command)
                    output = output.strip()

                    logging.debug(type(output))
                    if str(output) != '1' and str(exit_code) == '0':
                        test_results[instance_name]['under-replicated'] = 'Found: ' + str(output) +\
                                                                          ' under-replicated partitions'
                        test_status = 1
                    elif str(output) == '1' and str(exit_code) == '0':
                        test_results[instance_name]['under-replicated'] = None
                    elif str(exit_code) != '0':
                        test_results[instance_name]['under-replicated'] = 'unable to check under-replicated partitions'

                    logging.info("Lets check the server.log for this node.")
                    command = "cat /kafka/logs/server.log |grep 'ERR' |wc -l"
                    log_output, exit_code = remote_ssh_command_execution(task_id, ip_address, command)
                    log_output = log_output.strip()

                    if str(log_output) != '0':
                        logging.info("Found ERROR in the log of node: " + str(ip_address))
                        test_results[instance_name]['log-errors'] = 'Found: ' + str(log_output) + \
                                                                    ' errors in server.log'
                        test_status = 1
                    else:
                        logging.info("no errors in the log of node: " + str(ip_address))
                        test_results[instance_name]['log-errors'] = None
                else:
                    test_results[instance_name]['log-errors'] = 'skipped'
                    test_results[instance_name]['under-replicated'] = 'skipped'
                    test_results[instance_name]['kafka-port-status'] = 'skipped'

        email_body = format_result_dict(test_results)
        if test_status == 0:
            status = 'Kafka Sanity test PASSED for cluster: ' + str(cluster_name) + ' at GCP project: '\
                     + str(project_name) + ' completed, no errors found'
            mail_dict = {"subject": "DI Manager: " + str(status),
                         "body": "Status: " + str(email_body)}
            status_code = 200
        else:
            status = 'Kafka Sanity test FAILED for cluster: ' + str(cluster_name) + ' at GCP project: ' \
                     + str(project_name) + ' completed, with ERRORS'
            mail_dict = {"subject": "DI Manager: " + str(status), "body": email_body}
            status_code = 201

        logging.info(status)
        update_task(str(task_id), 'Finished', status_code, str(email_body))
        sendemail(target_mail_address, mail_dict)

    except Exception as e:
        logging.error('error: ' + str(e))

        message = 'Task ID: ' + str(task_id) + ' Kafka Sanity test ERROR for cluster: ' + str(cluster_name) + \
                  ' at GCP project: ' + str(project_name) + ' with error: ' + str(e)
        logging.info(message)
        sendemail(target_mail_address, {"subject": message, "body": str(test_results)})

        update_task(str(task_id), 'Finished', 500, str(e)[:65535], str(test_results))


def format_result_dict(dict_text):
    """
    accept a dict with the result dict of a Sanity check and format it to string with different line for each value
    :param dict_text
    :return: formatted text
    """
    email_failed = ''
    email_success = ''
    for key, value in dict_text.items():
        test_status = 0
        for key1, value1 in value.items():
            if key1 != 'ip address' and value1 is not None:
                test_status = 1
        if test_status == 0:
            email_success = email_success + key + '=' + str(value) + '\n'
        else:
            email_failed = email_failed + key + '=' + str(value) + '\n'
    output_text = 'Fail List:\n' + email_failed + '\n************************\nSuccess list:\n' + email_success
    logging.debug(output_text)
    return output_text


def sanity_elastic_cluster(task_id, payload):
    """
    Perform a sanity check of a elastic cluster:
    For each node execute:
    Check the cluster status green/yellow/red
    Check if the elastic port is open and responsive (9200)
    Check if the ssh port(22) is open and responsive
    Check if server.log contain ERRORS
    :param task_id:
    :param payload:
    """
    
    logging.info('This is my task_id: ' + str(task_id))

    def execute_elastic_tests(task_id, ip_address, instance_name, test_results, test_status):
        logging.info('Lets check if port 9200 for ssh is open')
        if port_check(ip_address, 9200):
            logging.info("elastic port: " + str(9200) + " is open at host: " + str(ip_address))
            test_results[instance_name]['elastic-port-status'] = None
        else:
            logging.info("elastic port: " + str(9200) + " is un-responsive at host: " + str(ip_address))
            test_results[instance_name]['elastic-port-status'] = 'port: ' + str(9200) + \
                                                                 ' is un-responsive'
            test_status = 1

        logging.info('Lets check if port 22 for ssh is open at elastic node')
        ssh_status = 0
        if port_check(ip_address, 22):
            logging.info("ssh port: 22 is open at host: " + str(ip_address))
            test_results[instance_name]['ssh-port-status'] = None
        else:
            logging.info("ssh port: 22 is un-responsive at host: " + str(ip_address))
            test_results[instance_name]['ssh-port-status'] = 'port: 22 is un-responsive'
            test_status = 1
            ssh_status = 1

        # the rest of the tests can not be executed without ssh access, so skipping.
        if ssh_status == 0:
            logging.info("Lets check the cluster status ")
            cluster_status = get_elastic_cluster_status(task_id, ip_address)
            logging.info('cluster status: ' + str(cluster_status))
            if str(cluster_status) == 'green':
                test_results[instance_name]['cluster-status'] = None
            else:
                test_results[instance_name]['cluster-status'] = str(cluster_status)
                test_status = 1

            logging.info("Lets check the elastic log for this node.")
            command = "cat /var/log/elasticsearch/" + cluster_name + ".log | grep 'ERROR' |wc -l"
            log_output, exit_code = remote_ssh_command_execution(task_id, ip_address, command)
            log_output = log_output.strip()

            if str(log_output) != '0':
                logging.info("Found ERROR in the log of node: " + str(ip_address))
                test_results[instance_name]['log-errors'] = 'Found: ' + str(log_output) + \
                                                            ' errors in elastic log'
                test_status = 1
            else:
                logging.info("no errors in the log of node: " + str(ip_address))
                test_results[instance_name]['log-errors'] = None
        else:
            test_results[instance_name]['cluster-status'] = 'skipped'
            test_results[instance_name]['log-errors'] = 'skipped'

        return test_status, test_results

    test_results = None
    target_mail_address = None
    project_name = None
    cluster_name = None
    test_status = 0
    try:
        project_id, project_name, target_mail_address, cluster_name, action, elastic_results_dict, full_results_dict = \
            rolling_action_parameters(task_id, payload, None, 'elastic')

        project_id, project_name, target_mail_address, cluster_name, action, client_results_dict, full_results_dict = \
            rolling_action_parameters(task_id, payload, None, 'client')

        project_id, project_name, target_mail_address, cluster_name, action, master_results_dict, full_results_dict = \
            rolling_action_parameters(task_id, payload, None, 'master')

        # Validate if project exists
        return_code, return_msg = check_project_exists(project_id)
        if return_code == 0 and return_msg == 'not exists':
            status = 'GCP Project: ' + str(project_id) + ' does not exists'
            logging.error(status)
            update_task(task_id, 'Finished', 202, status)
            return None

        check_kill_status(task_id)
        gitcheckout_exit_code, temp_folder = gitcheckout('master', 'elastic_git_path')
        project_dir = os.path.join(temp_folder, 'projects', project_name, cluster_name)
        # Check that the cluster directory exists, ie that the cluster exists
        if not os.path.isdir(project_dir):
            status = ('Cluster: ' + str(cluster_name) + ' does not exists or is not managed by DI manager')
            logging.error(status)
            update_task(task_id, 'Finished', 202, status)
            return None

        elastic_results_dict = {'elastic':  elastic_results_dict}
        master_results_dict = {'master':  master_results_dict}
        client_results_dict = {'client':  client_results_dict}
        results_dict = elastic_results_dict | master_results_dict | client_results_dict

        test_results = {}
        logging.info('Now checking elastic data nodes')
        for key1, value1 in results_dict.items():
            for key, value in sorted(value1.items(), reverse=True):
                check_kill_status(task_id)
                logging.debug("key: " + str(key))
                logging.debug("value: " + str(value))
                if key != -9999:
                    instance_name = get_instance_from_instance_group(task_id, value['instance group'], value['zone'],
                                                                     project_id)
                    instance_name = instance_name.strip()
                    ip_address = get_ip_from_instance(task_id, instance_name)
                    ip_address = ip_address.strip()
                    test_results[instance_name] = {}
                    test_results[instance_name]['ip address'] = ip_address

                    test_status, test_results = execute_elastic_tests(task_id, ip_address, instance_name,
                                                                      test_results, test_status)

        email_body = format_result_dict(test_results)
        if test_status == 0:
            status = 'elastic Sanity test PASSED for cluster: ' + str(cluster_name) + ' at GCP project: ' \
                     + str(project_name) + ' completed, no errors found'
            mail_dict = {"subject": "DI Manager: " + str(status),
                         "body": "Status: " + str(email_body)}
            status_code = 200
        else:
            status = 'elastic Sanity test FAILED for cluster: ' + str(cluster_name) + ' at GCP project: ' \
                     + str(project_name) + ' completed, with ERRORS'
            mail_dict = {"subject": "DI Manager: " + str(status), "body": email_body}
            status_code = 201

        logging.info(status)
        update_task(str(task_id), 'Finished', status_code, str(email_body))
        sendemail(target_mail_address, mail_dict)

    except Exception as e:
        logging.error('error: ' + str(e))

        message = 'Task ID: ' + str(task_id) + ' Elastic Sanity test ERROR for cluster: ' + str(cluster_name) + \
                  ' at GCP project: ' + str(project_name) + ' with error: ' + str(e)
        logging.info(message)
        sendemail(target_mail_address, {"subject": message, "body": str(test_results)})

        update_task(str(task_id), 'Finished', 500, str(e)[:65535], str(test_results))


def sanity_aerospike_cluster(task_id, payload):
    """
    Perform a sanity check of aerospike cluster:
    For each node execute:
    Check the cluster stability
    Check if the aerospike port is open and responsive
    Check if the ssh port(22) is open and responsive
    Check if server.log contain ERRORS
    :param task_id:
    :param payload:
    """
    
    logging.info('This is my task_id: ' + str(task_id))

    def execute_aerospike_tests(task_id, ip_address, instance_name, test_results, test_status):
        cfg_results = getdefaultvalues()
        aerospike_port = cfg_results.get('aerospike_service_port')
        logging.info('Lets check if port ' + str(aerospike_port) + ' for aerospike is open')
        if port_check(ip_address, aerospike_port):
            logging.info("aerospike port: " + str(aerospike_port) + " is open at host: " + str(ip_address))
            test_results[instance_name]['aerospike-port-status'] = None
        else:
            logging.info("aerospike port: " + str(aerospike_port) + " is un-responsive at host: " + str(ip_address))
            test_results[instance_name]['aerospike-port-status'] = 'port: ' + str(aerospike_port) + \
                                                                 ' is un-responsive'
            test_status = 1

        logging.info('Lets check if port 22 for ssh is open at aerospike node')
        ssh_status = 0
        if port_check(ip_address, 22):
            logging.info("ssh port: 22 is open at host: " + str(ip_address))
            test_results[instance_name]['ssh-port-status'] = None
        else:
            logging.info("ssh port: 22 is un-responsive at host: " + str(ip_address))
            test_results[instance_name]['ssh-port-status'] = 'port: 22 is un-responsive'
            test_status = 1
            ssh_status = 1

        # The rest of the tests cannot be executed without ssh access
        if ssh_status == 0:
            logging.info('Checking cluster status')
            aerospike_cluster_size = 0
            logging.info('first checking cluster size')
            for key, value in results_dict.items():
                if key != -9999:
                    aerospike_cluster_size += 1
            logging.info('current_size: ' + str(aerospike_cluster_size))
            command = "asadm --enable -e  \"asinfo -v \'cluster-stable:size=" + str(aerospike_cluster_size) + \
                      ";ignore-migrations=no\'\" | grep 'ERROR' | head -1 | awk -F '::' '{print $2}'"

            output, exit_code = remote_ssh_command_execution(task_id, ip_address, command)

            if len(output) != 0:
                logging.info("Aerospike cluster: " + str(cluster_name) + " cluster is not stable")
                test_results[instance_name]['cluster_stable'] = 'cluster unstable'
                test_status = 1
            else:
                logging.info("Aerospike cluster: " + str(cluster_name) + " cluster is stable")
                test_results[instance_name]['cluster_stable'] = None

            logging.info("Checking migration status")
            migration_status = check_migrations(task_id, ip_address)
            if migration_status == 0:
                logging.info("Aerospike cluster: " + str(cluster_name) + " no migration")
                test_results[instance_name]['cluster_migration'] = None
            elif migration_status == 10:
                logging.info("Aerospike cluster: " + str(cluster_name) + " migration in progress")
                test_results[instance_name]['cluster_migration'] = 'migration in progress'
                test_status = 1
            elif migration_status == 1:
                logging.info("Aerospike cluster: " + str(cluster_name) + " migration Failed")
                test_results[instance_name]['cluster_migration'] = 'migration failed'
                test_status = 1

            logging.info("Lets check the aerospike.log for this node.")
            command = "cat /usr/bin/aerospike/aerospike.log  |grep 'ERROR' |wc -l"
            log_output, exit_code = remote_ssh_command_execution(task_id, ip_address, command)
            log_output = log_output.strip()

            if str(log_output) != '0':
                logging.info("Found ERROR in the log of node: " + str(ip_address))
                test_results[instance_name]['log-errors'] = 'Found: ' + str(log_output) + \
                                                            ' errors in aerospike.log'
                test_status = 1
            else:
                logging.info("no errors in the log of node: " + str(ip_address))
                test_results[instance_name]['log-errors'] = None
        else:
            test_results[instance_name]['cluster_migration'] = 'skipped'
            test_results[instance_name]['log-errors'] = 'skipped'
            test_results[instance_name]['cluster_stable'] = 'skipped'

        return test_status, test_results

    test_results = None
    target_mail_address = None
    project_name = None
    cluster_name = None
    test_status = 0
    try:
        project_id, project_name, target_mail_address, cluster_name, action, results_dict, full_results_dict = \
            rolling_action_parameters(task_id, payload, None, 'aerospike')

        # Validate if project exists
        return_code, return_msg = check_project_exists(project_id)
        if return_code == 0 and return_msg == 'not exists':
            status = 'GCP Project: ' + str(project_id) + ' does not exists'
            logging.error(status)
            update_task(task_id, 'Finished', 202, status)
            return None

        check_kill_status(task_id)
        gitcheckout_exit_code, temp_folder = gitcheckout('master', 'aerospike_git_path')
        project_dir = os.path.join(temp_folder, 'projects', project_name, cluster_name)
        # Check that the cluster directory exists, ie that the cluster exists
        if not os.path.isdir(project_dir):
            status = ('Cluster: ' + str(cluster_name) + ' does not exists or is not managed by DI manager')
            logging.error(status)
            update_task(task_id, 'Finished', 202, status)
            return None

        test_results = {}
        logging.info('Now checking Aerospike data nodes')
        for key, value in sorted(results_dict.items(), reverse=True):
            check_kill_status(task_id)
            logging.debug("key: " + str(key))
            logging.debug("value: " + str(value))
            if key != -9999:
                instance_name = get_instance_from_instance_group(task_id, value['instance group'], value['zone'],
                                                                 project_id)
                instance_name = instance_name.strip()
                ip_address = get_ip_from_instance(task_id, instance_name)
                ip_address = ip_address.strip()
                test_results[instance_name] = {}
                test_results[instance_name]['ip address'] = ip_address

                test_status, test_results = execute_aerospike_tests(task_id, ip_address, instance_name,
                                                                  test_results, test_status)

        email_body = format_result_dict(test_results)
        if test_status == 0:
            status = 'aerospike Sanity test PASSED for cluster: ' + str(cluster_name) + ' at GCP project: ' \
                     + str(project_name) + ' completed, no errors found'
            mail_dict = {"subject": "DI Manager: " + str(status),
                         "body": "Status: " + str(email_body)}
            status_code = 200
        else:
            status = 'aerospike Sanity test FAILED for cluster: ' + str(cluster_name) + ' at GCP project: ' \
                     + str(project_name) + ' completed, with ERRORS'
            mail_dict = {"subject": "DI Manager: " + str(status), "body": email_body}
            status_code = 201

        logging.info(status)
        update_task(str(task_id), 'Finished', status_code, str(email_body))
        sendemail(target_mail_address, mail_dict)

    except Exception as e:
        logging.error('error: ' + str(e))

        message = 'Task ID: ' + str(task_id) + ' aerospike Sanity test ERROR for cluster: ' + str(cluster_name) + \
                  ' at GCP project: ' + str(project_name) + ' with error: ' + str(e)
        logging.info(message)
        sendemail(target_mail_address, {"subject": message, "body": str(test_results)})

        update_task(str(task_id), 'Finished', 500, str(e)[:65535], str(test_results))
