import hashlib
import logging

from di_app_manager.app_code.kafka_rolling_api import *


def parse_output(parsed_string, failed_string):
    parsed_counter = 0
    if failed_string in parsed_string:
        parsed_counter = 1
    return parsed_counter


def enable_xdr_replication_between_clusters(task_id, set_name, source_hostname, target_hostname, env, dest_cluster_name, source_cluster_name):
    """

    :param set_name:
    :param target_hostname:
    :param source_hostname:
    :param source_cluster_name:
    :param dest_cluster_name:
    :param env:
    :param task_id:
    :return:
    """

    try:
        xdr_total_exit_code = 0
        asadm_command = 'asadm --config-file /etc/aerospike/agm/as.conf --enable -e '
        datacenter = dest_cluster_name + '_' + set_name
        # Datacenter name is limited to 31 chars, set name to 63 chars, so i needed to create a DC name
        # That is deterministic and limited to X amount of chars (12)
        datacenter = "dc" + str(int(hashlib.sha1(datacenter.encode("utf-8")).hexdigest(), 16) % (10 ** 12))
        command = asadm_command + ' "manage config xdr create dc ' + datacenter + '"'

        # First check if port 3010 is open between source and dest nodes
        check_port = 'timeout 10 bash -c \'echo > /dev/tcp/' + target_hostname + '/3010 && echo "Port is open"\''
        port_output, port_exit_code = remote_ssh_command_execution(task_id, source_hostname, check_port)
        port_output = port_output.strip()
        if "Port is open" != port_output:
            status = 'unable to reach target_hostname: ' + target_hostname + ':3010' + \
                     ' from source_hostname: ' + source_hostname
            logging.error(status)
            return status, 400

        # Verify ENV type
        env = env.lower()
        if not env == 'dev' and not env == 'dr' and not env == 'prod' and not env == 'csdev':
            status = str('env types must be either: prod, dr, dev or csdev')
            logging.error(status)
            return status, 201

        # Create DC
        xdr_output, xdr_exit_code = remote_ssh_command_execution(task_id, source_hostname, command)
        xdr_total_exit_code += xdr_exit_code + parse_output(xdr_output, '|Failed')
        logging.debug('xdr_total_exit_code: ' + str(xdr_total_exit_code))
        # Check if DC already exists, if so exit
        if parse_output(xdr_output, 'DC already exists'):
            status = 'This XDR replication already exists: ' + str(datacenter)
            logging.error(status)
            return status, 201

        command = asadm_command + ' "manage config xdr dc ' + datacenter + ' param tls-name to ' + source_cluster_name + '"'
        xdr_output, xdr_exit_code = remote_ssh_command_execution(task_id, source_hostname, command)
        xdr_total_exit_code += xdr_exit_code + parse_output(xdr_output, '|Failed')
        logging.debug('xdr_total_exit_code: ' + str(xdr_total_exit_code))

        command = asadm_command + ' "manage config xdr dc ' + datacenter + ' param auth-mode to internal"'
        xdr_output, xdr_exit_code = remote_ssh_command_execution(task_id, source_hostname, command)
        xdr_total_exit_code += xdr_exit_code + parse_output(xdr_output, '|Failed')
        logging.debug('xdr_total_exit_code: ' + str(xdr_total_exit_code))

        command = asadm_command + ' "manage config xdr dc ' + datacenter + ' param auth-user to aerospike-super-' + env + '"'
        xdr_output, xdr_exit_code = remote_ssh_command_execution(task_id, source_hostname, command)
        xdr_total_exit_code += xdr_exit_code + parse_output(xdr_output, '|Failed')
        logging.debug('xdr_total_exit_code: ' + str(xdr_total_exit_code))

        command = asadm_command + ' "manage config xdr dc ' + datacenter + ' param auth-password-file to /etc/aerospike/super.txt"'
        xdr_output, xdr_exit_code = remote_ssh_command_execution(task_id, source_hostname, command)
        xdr_total_exit_code += xdr_exit_code + parse_output(xdr_output, '|Failed')
        logging.debug('xdr_total_exit_code: ' + str(xdr_total_exit_code))

        command = asadm_command + ' "manage config xdr dc ' + datacenter + ' namespace ' + source_cluster_name + ' param remote-namespace to ' + dest_cluster_name + '"'
        xdr_output, xdr_exit_code = remote_ssh_command_execution(task_id, source_hostname, command)
        xdr_total_exit_code += xdr_exit_code + parse_output(xdr_output, '|Failed')
        logging.debug('xdr_total_exit_code: ' + str(xdr_total_exit_code))

        command = asadm_command + ' \'asinfo -v "set-config:context=xdr;dc=' + datacenter + ';namespace=' + source_cluster_name + ';ship-nsup-deletes=true"\''
        xdr_output, xdr_exit_code = remote_ssh_command_execution(task_id, source_hostname, command)
        xdr_total_exit_code += xdr_exit_code + parse_output(xdr_output, '|Failed')
        logging.debug('xdr_total_exit_code: ' + str(xdr_total_exit_code))

        command = asadm_command + ' "manage config xdr dc ' + datacenter + ' namespace ' + source_cluster_name + ' param ship-only-specified-sets to true"'
        xdr_output, xdr_exit_code = remote_ssh_command_execution(task_id, source_hostname, command)
        xdr_total_exit_code += xdr_exit_code + parse_output(xdr_output, '|Failed')
        logging.debug('xdr_total_exit_code: ' + str(xdr_total_exit_code))

        command = asadm_command + ' "manage config xdr dc ' + datacenter + ' namespace ' + source_cluster_name + ' param ship-set to ' + set_name + '"'
        xdr_output, xdr_exit_code = remote_ssh_command_execution(task_id, source_hostname, command)
        xdr_total_exit_code += xdr_exit_code + parse_output(xdr_output, '|Failed')
        logging.debug('xdr_total_exit_code: ' + str(xdr_total_exit_code))

        command = asadm_command + ' "manage config xdr dc ' + datacenter + ' add node ' + target_hostname + ':3010:' + dest_cluster_name + '"'
        xdr_output, xdr_exit_code = remote_ssh_command_execution(task_id, source_hostname, command)
        xdr_total_exit_code += xdr_exit_code + parse_output(xdr_output, '|Failed')
        logging.debug('xdr_total_exit_code: ' + str(xdr_total_exit_code))

        command = asadm_command + ' "manage config xdr dc ' + datacenter + ' add namespace ' + source_cluster_name + ' rewind all"'
        xdr_output, xdr_exit_code = remote_ssh_command_execution(task_id, source_hostname, command)
        xdr_total_exit_code += xdr_exit_code + parse_output(xdr_output, '|Failed')
        logging.debug('xdr_total_exit_code: ' + str(xdr_total_exit_code))

        if xdr_total_exit_code == 0:
            status = 'XDR replication between ' + source_cluster_name + ' to: ' + dest_cluster_name + ' enabled.'
            logging.info(status)
            http_code = 200
        else:
            status = 'XDR replication failed to be enabled: ' + datacenter
            http_code = 400
        return status, http_code

    except Exception as e:
        logging.error('error: ' + str(e))
        status = 'Failed to enable XDR replication: ' + ' Internal error: ' + str(e)
        http_code = 500
        return status, http_code


def delete_xdr_replication_between_clusters(task_id, source_hostname, dest_cluster_name, source_cluster_name, set_name):
    """

    :param set_name:
    :param source_hostname:
    :param source_cluster_name:
    :param dest_cluster_name:
    :param task_id:
    :return:
    """

    try:
        xdr_total_exit_code = 0
        asadm_command = 'asadm --config-file /etc/aerospike/agm/as.conf --enable -e '
        datacenter = dest_cluster_name + '_' + set_name
        # Datacenter name is limited to 31 chars, set name to 63 chars, so i needed to create a DC name
        # That is deterministic and limited to X amount of chars (12)
        datacenter = "dc" + str(int(hashlib.sha1(datacenter.encode("utf-8")).hexdigest(), 16) % (10 ** 12))

        command = asadm_command + ' "manage config xdr delete dc ' + datacenter + '"'
        xdr_output, xdr_exit_code = remote_ssh_command_execution(task_id, source_hostname, command)
        xdr_total_exit_code += xdr_exit_code + parse_output(xdr_output, '|Failed')
        if parse_output(xdr_output, 'DC already exists'):
            status = 'This XDR replication doesnt exists: ' + str(datacenter)
            logging.error(status)
            return status, 400

        if xdr_total_exit_code == 0:
            status = 'XDR replication between ' + source_cluster_name + ' to: ' + dest_cluster_name + ' deleted.'
            logging.info(status)
            http_code = 200
        else:
            status = 'XDR replication failed to be deleted: ' + datacenter
            http_code = 400
        return status, http_code

    except Exception as e:
        logging.error('error: ' + str(e))
        status = 'Failed to delete XDR replication: ' + ' Internal error: ' + str(e)
        http_code = 500
        return status, http_code


def check_sets_status(task_id, source_hostname, target_hostname, dest_cluster_name, source_cluster_name, set_name):
    """

    :param target_hostname:
    :param set_name:
    :param source_hostname:
    :param source_cluster_name:
    :param dest_cluster_name:
    :param task_id:
    :return:
    """

    try:
        compare_total_exit_code = 0
        asadm_command = 'asadm --config-file /etc/aerospike/agm/as.conf --enable -e '

        source_command = asadm_command + ' "asinfo -v sets/' + source_cluster_name + '/' + set_name + '"'
        source_output, source_exit_code = remote_ssh_command_execution(task_id, source_hostname, source_command)
        compare_total_exit_code += source_exit_code
        source_objects = 0
        source_output = source_output.split('\n')
        for i in source_output:
            logging.debug(i)
            if 'objects' in i.strip():
                source_object = int((i.split(':')[0]).split('=')[1])
                source_objects += source_object
        logging.debug('source_objects: ' + str(source_objects))
        logging.debug(type(source_objects))

        dest_command = asadm_command + ' "asinfo -v sets/' + dest_cluster_name + '/' + set_name + '"'
        dest_output, dest_exit_code = remote_ssh_command_execution(task_id, target_hostname, dest_command)
        compare_total_exit_code += dest_exit_code
        dest_objects = 0
        dest_output = dest_output.split('\n')
        for i in dest_output:
            logging.debug(i)
            if 'objects' in i.strip():
                dest_output = int((i.split(':')[0]).split('=')[1])
                dest_objects += dest_output
        logging.debug('dest_objects: ' + str(dest_objects))
        logging.debug(type(dest_objects))

        if compare_total_exit_code != 0:
            status = 'Set: ' + str(set_name) + ' Failed to compare set between clusters'
            logging.warning(status)
            http_code = 400
        elif dest_objects == 0 and source_objects == 0:
            status = 'Set: ' + str(set_name) + ' is empty or does not exists in both clusters'
            logging.info(status)
            http_code = 201
        elif dest_objects == source_objects:
            status = 'Set: ' + str(set_name) + ' has identical object count between ' + source_cluster_name + ' to: ' \
                     + dest_cluster_name + ', object count: ' + str(dest_objects)
            logging.info(status)
            http_code = 200
        else:
            delta_objects = int(source_objects)-int(dest_objects)
            logging.debug('delta_objects:' + str(delta_objects))
            status = 'Set: ' + str(set_name) + ' has a different object count between source (' + str(source_objects) \
                     + ') and dest (' + str(dest_objects) + ') The delta is: ' + str(delta_objects)
            logging.warning(status)
            http_code = 400

        return status, http_code

    except Exception as e:
        logging.error('error: ' + str(e))
        status = 'Failed to identical object count: ' + ' Internal error: ' + str(e)
        http_code = 500
        return status, http_code


def truncate_set_aerospike(task_id, source_hostname, source_cluster_name, set_name):
    """
    Truncate Set in aerospike

    :param set_name:
    :param source_cluster_name:
    :param source_hostname:
    :param task_id:
    :return:
    """

    try:
        xdr_total_exit_code = 0
        asadm_command = 'asadm --config-file /etc/aerospike/agm/as.conf --enable -e '

        # Let's verify that the set exists
        exists_command = asadm_command + ' \'info set\' |grep ' + set_name + '| wc -l'
        exists_output, exists_exit_code = remote_ssh_command_execution(task_id, source_hostname, exists_command)
        if exists_exit_code == 0:
            if str(exists_output.strip()) == str(0):
                status = ('set ' + set_name + ' at cluster: ' + source_cluster_name + ' does not exists')
                logging.debug(status)
                http_code = 203
                return status, http_code
        else:
            status = ('unable to verify set ' + set_name + ' at cluster: ' + source_cluster_name + ' exists, check logs')
            logging.debug(status)
            http_code = 500
            return status, http_code

        command = asadm_command + ' \'asinfo -v "truncate:namespace=' + source_cluster_name + ';set=' + set_name + ';"\''
        xdr_output, xdr_exit_code = remote_ssh_command_execution(task_id, source_hostname, command)
        xdr_total_exit_code += xdr_exit_code + parse_output(xdr_output, '|Failed')

        if xdr_total_exit_code == 0:
            status = 'Aerospike Set at cluster ' + source_cluster_name + ' named: ' + set_name + ' Truncated.'
            logging.info(status)
            http_code = 200
        else:
            status = 'Aerospike Set failed to be Truncated: ' + set_name
            http_code = 400
        update_task(str(task_id), 'Finished', http_code, str(xdr_total_exit_code), str(status))
        return status, http_code

    except Exception as e:
        logging.error('error: ' + str(e))
        status = 'Aerospike Truncate set: ' + ' Internal error: ' + str(e)
        http_code = 500
        update_task(str(task_id), 'Finished', http_code, str(1), str(status))
        return status, http_code
