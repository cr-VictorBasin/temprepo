from di_app_manager.app_code.kafka_rolling_api import remote_ssh_command_execution
from di_app_manager.app_code.utils import *
import re
import ast


def create_topic_api(task_id, hostname, kafka_port, topic_name, replication_factor, partitions_num, configs):
    """
    This function allows us to create a new topic in target Kafka cluster
    :param configs: optional send the parameter config
    :param kafka_port: target kafka cluster port - most likely 9093
    :param task_id:
    :param hostname: the target hostname/IP
    :param topic_name: the new topic name
    :param replication_factor: the new topic RF - should be 1,2 or 3
    :param partitions_num: a valid number between 1-100
    :return:
    """
    logging.info('This is my task_id: ' + str(task_id))
    create_exit_code = 1
    http_code = 200
    status = ''
    config_command = ''

    logging.debug(configs)
    if configs != 'None':
        logging.debug(configs)
        logging.debug(type(configs))
        configs = ast.literal_eval(configs)
        for i in configs:
            string = '--config ' + i['name'] + '=' + i['value'] + ' '
            config_command += string
        config_command = config_command[:-1]
        logging.debug(config_command)

    def special_match(name, search=re.compile(r'[^a-zA-Z0-9\\._\\-]').search):
        return not bool(search(name))

    try:
        # Parameters validations
        if int(replication_factor) not in (1, 2, 3):
            status = 'replication_factor ' + str(replication_factor) + ' is not valid, valid value is 1,2 or 3'
            http_code = 400
        if 0 >= int(partitions_num) or int(partitions_num) >= 1001:
            status = 'partitions_num ' + str(partitions_num) + ' is not valid, valid value should be between 1-1000'
            http_code = 400
        if not special_match(topic_name):
            status = 'topic name ' + str(topic_name) + ' invalid, topic name can only include: [^a-zA-Z0-9\\._\\-]'
            http_code = 400
        if len(topic_name) > 255:
            status = 'topic name ' + str(topic_name) + ' invalid, topic length cannot exceed 255 chars.'
            http_code = 400
        if not port_check(hostname, 22):
            status = 'hostname ' + str(topic_name) + ' must be reachable via port 22 ssh.'
            http_code = 400

        if http_code == 400:
            update_task(str(task_id), 'Finished', http_code, str(1), str(status))
            return status, http_code

        # Resolve hostname to ip (acl needs ip)
        try:
            node_ip = socket.gethostbyname(hostname)
        except Exception as e:
            logging.error('error: ' + str(e))
            status = 'Unable to resolve ' + str(hostname) + ' Internal error: ' + str(e)
            http_code = 400
            update_task(str(task_id), 'Finished', http_code, str(1), str(status))
            return status, http_code
        logging.debug('node_ip: ' + str(node_ip))

        logging.debug('Validations completed')
        # prepare the create topic command
        command = 'sudo /opt/kafka/bin/kafka-topics.sh --create ' \
                  '--bootstrap-server ' + hostname + ':' + kafka_port + ' --replication-factor ' + replication_factor + \
                  ' --partitions ' + partitions_num + ' --topic ' + topic_name + \
                  ' --command-config /opt/kafka/ssl/client.properties '
        logging.info('About to execute the following create topic command: ')
        logging.info(command)
        if config_command != '':
            command = command + config_command
            logging.info('updated command: ' + command)
        create_output, create_exit_code = remote_ssh_command_execution(task_id, hostname, command)

        logging.debug('create exit code is: ' + str(create_exit_code))
        logging.debug('create output is: ' + str(create_output))

        if create_exit_code == 0:
            status = 'New Kafka Topic created ' + str(topic_name) + ' at hostname: ' + str(hostname) \
                     + ' finished successfully.'
            http_code = 200
        elif 'already exists.' in create_output:
            status = 'Kafka Topic ' + str(topic_name) + ' at hostname: ' + str(hostname) \
                     + ' already exists.'
            http_code = 201
        else:
            status = 'Failed ' + str(create_output) \
                     + ' failed with exit code: ' + str(create_exit_code)
            http_code = 400

        update_task(str(task_id), 'Finished', http_code, str(create_exit_code), str(status))
        return status, http_code

    except Exception as e:
        logging.error('error: ' + str(e))
        status = 'Failed to create new Kafka Topic: ' + str(topic_name) + ' at hostname ' + str(hostname) \
                 + ' Internal error: ' + str(e)
        http_code = 500
        update_task(str(task_id), 'Finished', http_code, str(create_exit_code), str(status))
        return status, http_code


def create_kafka_acl(task_id, hostname, customer_name, topic_name, operations_list, scope_list):
    """
    Create new ACL record for kafka cluster
    :param customer_name: the name of the customer as in the cert
    :param topic_name: target topic name, can have wildcards
    :param task_id:
    :param hostname: target hostname FQDN
    :param operations_list: valid values: ('Read', 'Write', 'Describe', 'Create', 'ClusterAction'):
    :param scope_list:
    :return:
    """
    logging.info('This is my task_id: ' + str(task_id))
    acl_exit_code = 1
    http_code = 200
    status = ''

    try:
        # Parameters validations
        if is_valid_IP_Address(hostname):
            status = 'target hostname ' + str(hostname) + ' must be a valid resolvable FQDN hostname.'
            http_code = 400
        if not hostname.endswith('cybereason.net'):
            status = 'target hostname ' + str(hostname) + ' must be a valid resolvable FQDN hostname,' \
                                                          ' under *.cybereason.net domain'
            http_code = 400

        if http_code == 400:
            update_task(str(task_id), 'Finished', http_code, str(1), str(status))
            return status, http_code

        # Resolve hostname to ip (acl needs ip)
        try:
            node_ip = socket.gethostbyname(hostname)
        except Exception as e:
            logging.error('error: ' + str(e))
            status = 'Unable to resolve ' + str(hostname) + ' Internal error: ' + str(e)
            http_code = 400
            update_task(str(task_id), 'Finished', http_code, str(1), str(status))
            return status, http_code
        logging.debug('node_ip: ' + str(node_ip))
        logging.debug('Validations completed')

        # parse Kafka server.properties and get zookeeper list  + port
        properties_file = '/opt/kafka/config/server.properties'
        zk_command = "sed -n -e '/^zookeeper.connect/p' " + properties_file
        zk_output, zk_exit_code = remote_ssh_command_execution(task_id, hostname, zk_command)
        zk_list = zk_output.replace('zookeeper.connect=', '')
        zk_list = zk_list.strip()
        logging.debug('zk_list: ' + str(zk_list))

        # Create operations_list
        operations_list = operations_list.split(',')
        operation_command = ''
        operation_prefix = '--operation'
        for operation in operations_list:
            if operation.lower() not in ('read', 'write', 'describe', 'create', 'clusteraction', 'delete', 'alter'):
                http_code = 400
                status = str(operation) + ' is not valid scope it can be only Read,Write,Describe,' \
                                          'Create,Delete,Alter or ClusterAction'
                update_task(str(task_id), 'Finished', http_code, str(acl_exit_code), str(status))
                return status, http_code
            if operation.lower() == 'clusteraction':
                operation = 'ClusterAction'
            else:
                operation = operation.capitalize()
            operation_command += operation_prefix + ' ' + operation + ' '
        logging.debug('operation_command: ' + str(operation_command))

        # prepare and parse scope command
        scope_list = scope_list.split(',')
        scope_command = ''
        for scope in scope_list:
            if scope.lower() == 'topic':
                line = ' --topic=' + topic_name
            elif scope.lower() == 'group':
                line = ' --group=* '
            elif scope.lower() == 'cluster':
                line = ' --cluster '
            else:
                http_code = 400
                status = str(scope) + ' is not valid scope it can be only topic,group or cluster'
                update_task(str(task_id), 'Finished', http_code, str(acl_exit_code), str(status))
                return status, http_code

            scope_command += line
        logging.debug('scope_command: ' + str(scope_command))

        # Let`s build the USER entry, this depends on the env PROD or the rest
        if hostname.endswith('prod.cybereason.net'):
            ou = 'Prod'
            s = 'Boston'
            c = 'MA'
        else:
            ou = 'cybersetup'
            s = 'TLV'
            c = 'IL'

        acl_user = 'User:CN=' + customer_name + ',OU=' + ou + ',O=Cybereason,ST=' + s + ',C=' + c
        logging.debug('acl_user: ' + str(acl_user))

        kafka_acl = '/opt/kafka/bin/kafka-acls.sh --authorizer-properties zookeeper.connect=' \
                    + zk_list + ' --add --allow-principal'
        logging.debug('kafka_acl: ' + str(kafka_acl))

        command = kafka_acl + ' ' + acl_user + ' ' + operation_command +\
                  ' ' + scope_command + ' 2</dev/null'
        logging.info('About to execute the following grant kafka ACL command: ')
        logging.info(command)
        acl_output, acl_exit_code = remote_ssh_command_execution(task_id, hostname, command)

        logging.debug('acl exit code is: ' + str(acl_exit_code))
        logging.debug('acl output is: ' + str(acl_output))

        if acl_exit_code == 0:
            status = 'ACL record created at hostname: ' + str(hostname) \
                     + ' finished successfully.'
            http_code = 200
        else:
            status = 'Failed ' + str(acl_output) \
                     + ' failed with exit code: ' + str(acl_exit_code)
            http_code = 400

        update_task(str(task_id), 'Finished', http_code, str(acl_exit_code), str(status))
        return status, http_code

    except Exception as e:
        logging.error('error: ' + str(e))
        status = 'Failed to create new ACL record: ' + ' Internal error: ' + str(e)
        http_code = 500
        update_task(str(task_id), 'Finished', http_code, str(acl_exit_code), str(status))
        return status, http_code


def delete_topic_api(task_id, hostname, kafka_port, topic_name):
    """
    delete target topic in target kafka cluster
    :param kafka_port: target kafka cluster port - most likely 9093
    :param task_id:
    :param hostname: the target hostname/FQDN
    :param topic_name: the new topic name
    :return:
    """
    logging.info('This is my task_id: ' + str(task_id))
    delete_exit_code = 1

    try:
        # Resolve hostname to ip (acl needs ip)
        try:
            node_ip = socket.gethostbyname(hostname)
        except Exception as e:
            logging.error('error: ' + str(e))
            status = 'Unable to resolve ' + str(hostname) + ' Internal error: ' + str(e)
            http_code = 400
            update_task(str(task_id), 'Finished', http_code, str(1), str(status))
            return status, http_code
        logging.debug('node_ip: ' + str(node_ip))

        # Parameters validations
        if len(topic_name) > 255:
            status = 'topic name ' + str(topic_name) + ' invalid, topic length cannot exceed 255 chars.'
            logging.info(status)
            http_code = 400
            return status, http_code
        if not port_check(node_ip, 22):
            status = 'hostname ' + str(topic_name) + ' must be reachable via port 22 ssh.'
            http_code = 400
            logging.info(status)
            return status, http_code

        # Make sure hostname/FQDN was provided and not IP
        try:
            socket.inet_aton(hostname)
            status = ('hostname ' + str(hostname) + ' provided is actually an IP address, should be FQDN')
            logging.error(status)
            http_code = 400
            return status, http_code
        except socket.error:
            logging.info('hostname is not an IP, moving forward')

        logging.debug('Validations completed')
        # prepare delete topic command
        command = 'sudo /opt/kafka/bin/kafka-topics.sh --delete ' \
                  '--bootstrap-server ' + hostname + ':' + kafka_port + \
                  ' --delete --topic ' + topic_name + \
                  ' --command-config /opt/kafka/ssl/client.properties'
        logging.info('About to execute the following create topic command: ')
        logging.info(command)
        delete_output, delete_exit_code = remote_ssh_command_execution(task_id, hostname, command)

        logging.debug('delete exit code is: ' + str(delete_exit_code))
        logging.debug('delete output is: ' + str(delete_output))

        if delete_exit_code == 0:
            status = 'Kafka Topic Deleted ' + str(topic_name) + ' at hostname: ' + str(hostname) \
                     + ' finished successfully.'
            http_code = 200
        elif 'does not exist as expected' in delete_output:
            status = 'Kafka Topic ' + str(topic_name) + ' at hostname: ' + str(hostname) \
                     + ' does not exist as expected.'
            http_code = 201
        else:
            status = 'Failed to execute command: ' + str(command) + ' at target host: ' + hostname
            http_code = 400

        update_task(str(task_id), 'Finished', http_code, str(delete_exit_code), str(status))
        return status, http_code

    except Exception as e:
        logging.error('error: ' + str(e))
        status = 'Failed to delete Kafka Topic: ' + str(topic_name) + ' at hostname ' + str(hostname) \
                 + ' Internal error: ' + str(e)
        http_code = 500
        update_task(str(task_id), 'Finished', http_code, str(delete_exit_code), str(status))
        return status, http_code


def modify_topic_config_api(task_id, hostname, kafka_port, topic_name, configs):
    """
    modify (overwrite) the config part of a kafka topic
    :param configs:
    :param kafka_port: target kafka cluster port - most likely 9093
    :param task_id:
    :param hostname: the target hostname/IP
    :param topic_name: the new topic name
    :return:
    """
    logging.info('This is my task_id: ' + str(task_id))
    modify_exit_code = 1
    http_code = 200
    status = ''
    try:
        logging.debug(configs)
        logging.debug(type(configs))
        configs = ast.literal_eval(configs)
        config_command = ''
        for i in configs:
            string = i['name'] + '=' + i['value'] + ','
            config_command += string
        config_command = config_command[:-1]
        logging.debug(config_command)

        if config_command == '':
            status = 'Kafka Topic ' + str(topic_name) + ' at hostname: ' + str(hostname) \
                     + ' no modification required.'
            http_code = 202
            return status, http_code

        grant_status, grant_code = grant_alter_acl(task_id, hostname, topic_name)
        if grant_code != 200:
            return grant_status, grant_code

        logging.debug('Validations completed')
        # prepare to modify topic command
        command = 'sudo /opt/kafka/bin/kafka-configs.sh --alter ' \
                  '--bootstrap-server ' + hostname + ':' + kafka_port + \
                  ' --entity-type topics --entity-name ' + topic_name + \
                  ' --add-config ' + config_command + \
                  ' --command-config /opt/kafka/ssl/client.properties'
        logging.info('About to execute the following modify topic command: ')
        logging.info(command)
        modify_output, modify_exit_code = remote_ssh_command_execution(task_id, hostname, command)

        logging.debug('create exit code is: ' + str(modify_exit_code))
        logging.debug('create output is: ' + str(modify_output))

        if modify_exit_code == 0:
            status = 'Kafka Topic ' + str(topic_name) + ' at hostname: ' + str(hostname) \
                     + ' modified successfully.'
            http_code = 200
        elif 'already exists.' in modify_output:
            status = 'Kafka Topic ' + str(topic_name) + ' at hostname: ' + str(hostname) \
                     + ' already exists.'
            http_code = 201
        else:
            status = 'Failed ' + str(modify_output) \
                     + ' failed with exit code: ' + str(modify_exit_code)
            http_code = 400

        update_task(str(task_id), 'Finished', http_code, str(modify_exit_code), str(status))
        return status, http_code

    except Exception as e:
        logging.error('error: ' + str(e))
        status = 'Failed to modify Kafka Topic: ' + str(topic_name) + ' at hostname ' + str(hostname) \
                 + ' Internal error: ' + str(e)
        http_code = 500
        update_task(str(task_id), 'Finished', http_code, str(modify_exit_code), str(status))
        return status, http_code


def alter_topic_partitions(task_id, hostname, kafka_port, topic_name, partition_num):
    """
    alter a topic partition number
    :param partition_num: the new target partition count
    :param kafka_port: target kafka cluster port - most likely 9093
    :param task_id:
    :param hostname: the target hostname/FQDN
    :param topic_name: the new topic name
    :return:
    """
    logging.info('This is my task_id: ' + str(task_id))
    alter_exit_code = 1

    try:
        # Resolve hostname to ip (acl needs ip)
        try:
            node_ip = socket.gethostbyname(hostname)
        except Exception as e:
            logging.error('error: ' + str(e))
            status = 'Unable to resolve ' + str(hostname) + ' Internal error: ' + str(e)
            http_code = 400
            update_task(str(task_id), 'Finished', http_code, str(1), str(status))
            return status, http_code
        logging.debug('node_ip: ' + str(node_ip))

        # Parameters validations
        if int(partition_num) < 1 or int(partition_num) > 1000:
            status = 'New partition target number should be a positive number between 1 <-> 1000'
            logging.info(status)
            http_code = 400
            return status, http_code

        # Make sure hostname/FQDN was provided and not IP
        try:
            socket.inet_aton(hostname)
            status = ('hostname ' + str(hostname) + ' provided is actually an IP address, should be FQDN')
            logging.error(status)
            http_code = 400
            return status, http_code
        except socket.error:
            logging.info('hostname is not an IP, moving forward')

        grant_status, grant_code = grant_alter_acl(task_id, hostname, topic_name)
        if grant_code != 200:
            return grant_status, grant_code

        logging.debug('Validations completed')
        # prepare alter topic command
        command = 'sudo /opt/kafka/bin/kafka-topics.sh ' \
                  '--bootstrap-server ' + hostname + ':' + kafka_port + \
                  ' --alter --topic ' + topic_name + \
                  ' --partitions ' + str(partition_num) + \
                  ' --command-config /opt/kafka/ssl/client.properties'
        logging.info('About to execute the following create topic command: ')
        logging.info(command)
        alter_output, alter_exit_code = remote_ssh_command_execution(task_id, hostname, command)

        logging.debug('alter exit code is: ' + str(alter_exit_code))
        logging.debug('alter output is: ' + str(alter_output))

        if alter_exit_code == 0:
            status = 'Kafka Topic altered ' + str(topic_name) + ' at hostname: ' + str(hostname) \
                     + ' finished successfully, new partition count: ' + str(partition_num)
            http_code = 200
        elif 'does not exist as expected' in alter_output:
            status = 'Kafka Topic ' + str(topic_name) + ' at hostname: ' + str(hostname) \
                     + ' does not exist as expected.'
            http_code = 201
        elif 'Topic already has' in alter_output:
            status = 'Kafka Topic ' + str(topic_name) + ' at hostname: ' + str(hostname) \
                     + ' already has ' + str(partition_num) + ' partitions'
            http_code = 202
        elif 'which is higher than the requested' in alter_output:
            status = 'Kafka Topic ' + str(topic_name) + ' at hostname: ' + str(hostname) \
                     + ', ' + alter_output
            http_code = 203
        else:
            status = 'Failed to execute command: ' + str(command) + ' with error: ' + str(alter_output)
            http_code = 400

        update_task(str(task_id), 'Finished', http_code, str(alter_exit_code), str(status))
        return status, http_code

    except Exception as e:
        logging.error('error: ' + str(e))
        status = 'Failed to alter Kafka Topic: ' + str(topic_name) + ' at hostname ' + str(hostname) \
                 + ' Internal error: ' + str(e)
        http_code = 500
        update_task(str(task_id), 'Finished', http_code, str(alter_exit_code), str(status))
        return status, http_code


def grant_alter_acl(task_id, hostname, topic_name):
    """
    grant ACL access to alter commands
    :param topic_name:
    :param task_id:
    :param hostname:
    :return:
    """
    acl_exit_code = ''
    logging.debug(':ets provide the ACL needed')
    # Let`s build the USER entry, this depends on the env PROD or the rest
    try:
        if hostname.endswith('prod.cybereason.net'):
            ou = 'Prod'
            s = 'Boston'
            c = 'MA'
            domain = 'prod.cybereason.net'
        else:
            ou = 'cybersetup'
            s = 'TLV'
            c = 'IL'
            domain = 'eng.cybereason.net'

        acl_user = 'User:CN=' + domain + ',OU=' + ou + ',O=Cybereason,ST=' + s + ',C=' + c
        logging.debug('acl_user: ' + str(acl_user))

        kafka_acl = "/opt/kafka/bin/kafka-acls.sh --authorizer-properties zookeeper.connect=" \
                    "$(grep zookeeper /opt/kafka/config/server.properties | awk -F '=' '{print $2}') " \
                    "--add --allow-principal " + acl_user + " --operation Alter --operation AlterConfigs --topic=*"
        logging.debug('kafka_acl: ' + str(kafka_acl))

        command = kafka_acl + ' 2</dev/null'

        logging.info('About to execute the following grant kafka ACL command: ')
        logging.info(command)
        acl_output, acl_exit_code = remote_ssh_command_execution(task_id, hostname, command)

        logging.debug('acl exit code is: ' + str(acl_exit_code))
        logging.debug('acl output is: ' + str(acl_output))

        if acl_exit_code == 0:
            status = 'ACL record created at hostname: ' + str(hostname) \
                     + ' finished successfully.'
            logging.info(status)
            http_code = 200
        else:
            status = 'Failed ' + str(acl_output) \
                     + ' failed with exit code: ' + str(acl_exit_code)
            logging.info(status)
            http_code = 400
        return status, http_code
    except Exception as e:
        logging.error('error: ' + str(e))
        status = 'Failed to alter Kafka Topic: ' + str(topic_name) + ' at hostname ' + str(hostname) \
                 + ' Internal error: ' + str(e)
        http_code = 500
        update_task(str(task_id), 'Finished', http_code, str(acl_exit_code), str(status))
        return status, http_code
