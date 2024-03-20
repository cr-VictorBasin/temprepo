import paramiko
import random
from di_app_manager.app_code.validation import *
from di_app_manager.app_code.kafka_rolling_api import remote_ssh_command_execution


@retry(stop=(stop_after_attempt(10)))
def get_random_instance_from_cluster(task_id, cluster_name, project_id):
    """
    Get random instance from given cluster and returns its IP address
    :param task_id:
    :param project_id:
    :param cluster_name:
    :return: IP address or NONE
    """
    try:
        cfg_results = getdefaultvalues()
        gcloud_path = cfg_results.get('gcloud_path')

        command = gcloud_path + " auth activate-service-account --key-file=/root/gcp_key.json; " + \
                  gcloud_path + " config set project " + project_id + "; " + \
                  gcloud_path + " compute instances list --filter=\"name~'kafka-cluster-" + cluster_name + "-*'\"" + \
                                " --format=\"value(networkInterfaces[0].networkIP)\""

        command = command.replace("\n", "")
        logging.debug('About to execute: ' + str(command))
        process = subprocess.run(command, capture_output=True, text=True, shell=True)
        output = process.stdout
        logging.debug("list of IPs is: " + str(output))

        if not output:
            logging.debug("List of IPs is empty")
            return None
        else:
            output_list = output.splitlines()
            kafka_ip = random.choice(output_list)
            logging.debug("Going to execute the command on: " + str(kafka_ip))
            return kafka_ip

    except Exception as e:
        logging.error('error: ' + str(e))
        update_task(str(task_id), 'Finished', 500, str(e)[:65535])
        sys.exit(1)


@retry(stop=(stop_after_attempt(10)))
def sftp_remote_file(host, source, dest):
    """
    Connect to a host and remotely execute a command, return the command output and the exit_code
    :param host: the target host to ftp the file from
    :param source: The target host file path to ftp
    :param dest: The local host file path to copy the file
    """
    client = None

    logging.info('About to SCP file: ' + str(source) + ' from host: ' + str(host) + ' to ' + dest)
    try:
        cfg_results = getdefaultvalues()
        private_key_file = cfg_results.get('private_key_file')
        username = cfg_results.get('os_username')
        enable_paramiko_debug = cfg_results.get('enable_paramiko_debug')
        if enable_paramiko_debug == 'no':
            logging.getLogger("paramiko").setLevel(logging.WARNING)
        else:
            logging.getLogger("paramiko").setLevel(logging.DEBUG)

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())

        while not client.get_transport():
            client.connect(host,
                           username=username,
                           key_filename=private_key_file,
                           allow_agent=False,
                           look_for_keys=False,
                           compress=True)

        sftp = client.open_sftp()
        logging.debug('Going to ftp ' + source + ' to ' + dest)
        sftp.get(source, dest)
        sftp.close()
        return 0

    except TimeoutError:
        status = "TimeoutError, no SSH access probably the node haven't started yet."
        logging.error(status)
        return 10, None
    except Exception as e:
        logging.error('error: ' + str(e))
        return 1
    finally:
        if client:
            client.close()


def balance_partitions(task_id, payload):
    """
    Accept request with project_id, cluster_name, action and target_mail_address, then perform balance partitions
    :param task_id:
    :param payload: user payload
    :return: balance output code and balance output stdout
    """
    
    logging.info('This is my task_id: ' + str(task_id))
    project_id = GetKeyValueFromDict(payload.data, 'project_id')
    project_name = get_project_name_from_id(project_id)
    target_mail_address = GetKeyValueFromDict(payload.data, 'target_mail_address')
    cluster_name = GetKeyValueFromDict(payload.data, 'cluster_name')
    max_partitions = GetKeyValueFromDict(payload.data, 'max_partitions')
    command = 'sudo -u centos /usr/bin/kf-tool/kf.sh -l y balance ' + max_partitions
    logging.info('The balance command is: ' + command)
    kf_log_path = "/tmp/kf.log"
    local_path = "/var/tmp/kf.log"
    attachment = None

    # Validate if project already exists
    return_code, return_msg = check_project_exists(project_id)
    if return_code == 0 and return_msg == 'not exists':
        status = 'GCP Project: ' + str(project_id) + ' does not exists'
        update_task(str(task_id), 'Finished', 202, str(status), None)

    try:
        ip_address = get_random_instance_from_cluster(task_id, cluster_name, project_id)
        logging.debug("Selected IP address is: " + str(ip_address))
        if ip_address is None:
            status = 'Kafka Partitions balance of cluster ' + str(project_name) + " " + str(cluster_name) + \
                     " finished unsuccessfully - No such cluster: "\
                     + " " + cluster_name + " on project " + project_name
            mail_dict = {"subject": "DI Manager: " + str(status), "body": str(payload.data) + os.linesep + os.linesep +
                         "Status: " + str(status)}
            update_task(str(task_id), 'Finished', 500, str(status))
            sendemail(target_mail_address, mail_dict)
        else:
            ip_address = ip_address.strip()
            logging.debug("IP address to use is: " + ip_address)

            check_kill_status(task_id)
            logging.info('Running balance partitions: ' + str(command))
            balance_output, balance_exit_code = remote_ssh_command_execution(task_id, ip_address, command)

            logging.debug('balance exit code is: ' + str(balance_exit_code))
            logging.debug('balance output is: ' + str(balance_output))

            if balance_exit_code == 0:
                status = 'Kafka Partitions balance of cluster ' + str(project_name) + " " + str(cluster_name)\
                         + ' finished successfully.'
            else:
                status = 'Kafka Partitions balance of cluster ' + str(project_name) + " " + str(cluster_name) + \
                         ' finished unsuccessfully with exit code: ' + str(balance_exit_code)
                sftp_error = sftp_remote_file(ip_address, kf_log_path, local_path)
                if sftp_error == 0:
                    if os.path.isfile(local_path):
                        logging.debug("kf.log file was found")
                        kf_log = open(local_path, "r")
                        attachment = kf_log.read()
                        logging.debug("kf.log output is: " + attachment)
                        kf_log.close()
                    else:
                        logging.debug("kf.log file was not found")
                        attachment = "ERROR: Was not able to retrieve the log file"
                else:
                    logging.debug("SFTP failed with error")

            mail_dict = {"subject": "DI Manager: " + str(status), "body": str(payload.data) + os.linesep + os.linesep +
                         "Status: " + str(status)}
            if attachment is None:
                sendemail(target_mail_address, mail_dict)
            else:
                sendemail(target_mail_address, mail_dict, attachment=str(attachment))
            update_task(str(task_id), 'Finished', 200, str(balance_exit_code), str(status))

    except Exception as e:
        logging.error('error: ' + str(e))
        status = 'Task ID: ' + str(task_id) + 'Kafka Partitions balance of cluster ' + str(project_name) + " "\
                 + str(cluster_name) + ' finished unsuccessfully.'
        mail_dict = {"subject": "DI Manager: " + str(status),
                     "body": str(payload.data) + os.linesep + os.linesep +
                     "Status: " + str(status) + "With exception: " + str(e)}
        sendemail(target_mail_address, mail_dict)
        update_task(str(task_id), 'Finished', 500, str(e)[:65535])
