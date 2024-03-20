import socket
import subprocess
import logging
from logging.handlers import RotatingFileHandler
import sys
import pickle
from configparser import ConfigParser
import os
from datetime import datetime, timedelta
from pathlib import Path
import shutil
import pytz
import smtplib
import json
from tenacity import *
from random import randrange
from di_app_manager.models import *
import requests
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import time
import asyncio
from rest_framework.response import Response


def sendemail(target_address, msg_info, attachment=None):
    logging.debug('target_address: ' + str(target_address) + ', msg_info: ' + str(msg_info))
    """
    send an email, smtp server info is under the utils.cfg file.

    :target_address string, target email address
    :msg_info Json, Json that include 2 keys, subject and body, ie: {"subject":"text subject","body":"text body"}
    :return status_code number (1,0), status_code  = 1 failed, status_code = 0 success
    :return error_info exception, if any Exception was raised return the Exception text
    """
    msg_body = GetKeyValueFromDict(msg_info, 'body')
    subject = GetKeyValueFromDict(msg_info, 'subject')
    try:
        cfg_results = getdefaultvalues()
        smtp_info = cfg_results.get('smtp_info')
        smtp_user = cfg_results.get('smtp_user')
        smtp_token = cfg_results.get('smtp_token')

        toaddr = target_address

        msg = MIMEMultipart()

        msg['From'] = smtp_user
        msg['To'] = toaddr
        msg['Subject'] = subject
        msg.attach(MIMEText(msg_body))

        if attachment is not None:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment)
            encoders.encode_base64(part)
            filename = 'di-manager.log'
            part.add_header('Content-Disposition', "attachment; filename= %s" % filename)

            msg.attach(part)

        server = smtplib.SMTP(smtp_info)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(smtp_user, smtp_token)
        text = msg.as_string()
        logging.info('About to send email: fromaddr: ' + smtp_user + ', toaddr: ' + toaddr + ', text: ' + text)
        server.sendmail(smtp_user, toaddr, text)
        server.quit()
        logging.info('Email successfully sent to: ' + toaddr)
        return 0, None
    except Exception as e:
        logging.error('error: ' + str(e))
        return 1, str(e)


def is_file_older_than(file, delta):
    """
    Check if a file creation time is older then delta

    :param file: file name
    :param delta: the delta time in seconds
    :return: True/False
    """
    cutoff = datetime.utcnow() - delta
    mtime = datetime.utcfromtimestamp(os.path.getmtime(file))
    if mtime < cutoff:
        return True
    return False


def config_model_to_dict(query_string):
    """
    convert Django Query set format to dict
    :param query_string: Django query set object
    :return: dict object
    """
    try:
        list_result = [entry for entry in query_string]
        config_dict = {}
        for testing in list_result:
            for key, value in testing.items():
                if key == 'parameter_name':
                    current_key = value
                elif key == 'parameter_value':
                    current_value = value
            config_dict[current_key] = current_value
        return config_dict
    except Exception as e:
        logging.error(str(e))


def get_vault_value(vault_elastic_url, key_url, key_name):
    """
    return the key value from vault base on provided info, by API call

    :param vault_elastic_url: the url of Vault, ie http://<vault-ip>:8200
    :param key_url: the vault key path, ie /v1/secret/data_infra_secerts/kafka
    :param key_name: the key name in Vault
    :return: return the key value from vault base on provided info
    """
    try:
        vault_token = os.environ["VAULT_TOKEN"]
        headers_dict = {"X-Vault-Token": vault_token}
        url = vault_elastic_url + key_url
        response = requests.get(url, headers=headers_dict)
        response_json = json.loads(response.content.decode('utf-8'))
        value = response_json['data'][key_name]
        return value
    except Exception as error:
        logging.error(error)
        return 1


def gather_configs():
    """
    provide a list of keys, the source of the values is the cfg file and any secrets manager

    :param args:
    :return: results_list, as a list of keys and values requested
    """
    results_dict = {}
    try:
        cfg_dir = os.path.join('/code', 'di_app_manager', 'utils.cfg')
        parser = ConfigParser()
        status = parser.read(cfg_dir)
        logging.debug('Reading parameters file: ' + str(status))

        # read the entire cfg file and convert it into a dict object
        for each_section in parser.sections():
            for (each_key, each_val) in parser.items(each_section):
                results_dict[each_key] = each_val
        logging.debug('current local cfg_dict: ' + str(results_dict))

        # Now lets read the *_config tables at the data infra database repo
        logging.debug('Gathering info from database repo _config tables')
        kafka_query = kafka_config.objects.values('parameter_name', 'parameter_value')
        common_query = common_config.objects.values('parameter_name', 'parameter_value')
        elastic_query = elastic_config.objects.values('parameter_name', 'parameter_value')
        vault_query = vault_config.objects.values('parameter_name', 'parameter_value')
        postgers_query = postgers_config.objects.values('parameter_name', 'parameter_value')
        aerospike_query = aerospike_config.objects.values('parameter_name', 'parameter_value')

        logging.debug('Convert queryset data format to dict')
        kafka_dict = config_model_to_dict(kafka_query)
        logging.debug('kafka_dict: ' + str(kafka_dict))
        common_dict = config_model_to_dict(common_query)
        logging.debug('common_dict: ' + str(common_dict))
        elastic_dict = config_model_to_dict(elastic_query)
        logging.debug('elastic_dict: ' + str(elastic_dict))
        vault_dict = config_model_to_dict(vault_query)
        logging.debug('vault_dict: ' + str(vault_dict))
        postgers_dict = config_model_to_dict(postgers_query)
        logging.debug('postgers_dict: ' + str(postgers_dict))
        aerospike_dict = config_model_to_dict(aerospike_query)
        logging.debug('aerospike_dict: ' + str(aerospike_dict))

        # Let`s read values from Vault
        logging.debug('Getting data from vault')
        git_password = get_vault_value(results_dict['vault_url'], common_dict['git_password_url'], 'password')

        # Now lets merge the dicts from all config tables into one.
        def mergedict(*args):
            output = {}
            for arg in args:
                output.update(arg)
            return output
        config_dict = mergedict(postgers_dict, vault_dict, elastic_dict, kafka_dict, aerospike_dict, common_dict)
        logging.debug('current repo config_dict: ' + str(config_dict))
        # Not lets merge cfg configs and database configs.
        results_dict.update(config_dict)

        # Replace Git username and Git passwords in the config dict
        logging.debug('Config results_dict is ready, printing and replacing: ')
        for k, v in results_dict.items():
            if '**git_password**' in v or '**git_username**' in v:
                results_dict[k] = (v.replace('**git_password**', git_password)).replace('**git_username**', results_dict['git_username'])
            logging.debug(str(k) + ': ' + str(v))

        # Let`s pickle it
        os.chdir('/code')
        pickle_file = "temp_cfg.p"
        pickle.dump(results_dict, open(pickle_file, "wb"))
        return results_dict
    except Exception as e:
        logging.error('unable to read config parameters with error: ' + str(e))
        return 1


@retry(stop=(stop_after_attempt(10)))
def getdefaultvalues():
    """
    provide a list of keys, the source of the values is the cfg file and any secrets manager

    :param args:
    :return: results_list, as a list of keys and values requested
    """
    try:

        os.chdir('/code')
        # CHeck if this is first run, if so read data from cfg and database repo
        if not os.path.exists("temp_cfg.p"):
            logging.debug('Could not find temp_cfg.p, reading configs from database and cfg file')
            results_dict = gather_configs()
            #logging.debug('returned the following result : ' + str(results_dict))
            if results_dict == 1:
                logging.error('Could not read configs from database, no temp_cfg.p file, couldnt fetch configs, aborting...')
                sys.exit(1)
            return results_dict

        cfg_pickle_path = Path("temp_cfg.p")
        # Check if pickle file exists and if it is not older then X amount of seconds, if so use it
        if cfg_pickle_path.is_file() and not is_file_older_than("temp_cfg.p", timedelta(seconds=300)):
            logging.debug('Found a recent temp_cfg.p, reusing')
            results_dict = pickle.load(open("temp_cfg.p", "rb"))
        else:
            logging.debug('Could not find a recent temp_cfg.p, re-reading configs from database and cfg file')
            results_dict = gather_configs()
            if results_dict == 1:
                logging.debug('Could not read configs from database, fallback to re-reading stale temp_cfg.p')
                results_dict = pickle.load(open("temp_cfg.p", "rb"))
        #logging.debug('returned the following result : ' + str(results_dict))
        return results_dict
    except Exception as e:
        logging.error('unable to read parameters with error: ' + str(e))
        sys.exit(1)


def terraform_action(current_dir, action, terraform_lock='no'):
    """
    Terraform/terragrunt <action> the current provided dir, action = init/plan/apply

    :return return code:
        0 = Succeeded with empty diff (no changes)
        1 = Error
        2 = Succeeded with non-empty diff (changes present):
    """
    try:
        terraform_output = None
        logging.info('About to Terraform ' + action + ' action')
        cfg_results = getdefaultvalues()
        terraform_executable = cfg_results.get('terraform_executable')
        orig_dir = os.getcwd()
        os.chdir(current_dir)

        def terraform_execute(terraform_command):
            try:
                my_env = os.environ.copy()
                my_env["GOOGLE_APPLICATION_CREDENTIALS"] = "/root/gcp_key.json"
                my_env["GOOGLE_CREDENTIALS"] = "/root/gcp_key.json"
                logging.debug(my_env)
                logging.info('Executing: ' + str(terraform_command))
                process = subprocess.Popen(
                    terraform_command,
                    env=my_env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    shell=True,
                    cwd=current_dir,
                    encoding='utf-8',
                    errors='replace'
                )
                command_output = ' '
                while True:
                    realtime_output = process.stdout.readline()
                    if realtime_output == '' and process.poll() is not None:
                        break
                    if realtime_output:
                        print(realtime_output.strip(), flush=True)
                        command_output = command_output + os.linesep + (realtime_output.strip())
                rc = process.returncode
                return rc, command_output
            except Exception as terraform_error:
                logging.error('error: ' + str(terraform_error))
                return 1, str(terraform_error)
        if terraform_lock == 'no':
            lock = '-lock=false'
        else:
            lock = ' '
        if action == 'init':
            command = [terraform_executable + ' ' + action + ' ' + lock]
        elif action == 'plan':
            command = [terraform_executable + ' ' + action + ' -detailed-exitcode -no-color ' + lock]
        elif action == 'apply' or action == 'destroy':
            command = [terraform_executable + ' ' + action + ' ' + lock + ' -auto-approve -no-color']
        else:
            command = [terraform_executable + ' ' + action + ' ' + lock + ' -auto-approve -no-color']
        action_return_code, terraform_output = terraform_execute(command)
        os.chdir(orig_dir)
        logging.debug(str(terraform_output)[-65535:])
        terraform_output = parse_terraform_output(terraform_output)
        logging.info('Terraform ' + action + ' ended with status code ' + str(action_return_code))
        return action_return_code, terraform_output
    except Exception as e:
        logging.error('error: ' + str(e))
        return 1, str(e)


def parse_terraform_output(terraform_output):
    """
    Parse the terraform full output and search for errors or string, it will then output a proper error/info message
    :param terraform_output: Full output of a terraform run
    :return: formated output error/info message that will be displayed in the code
    """
    if "you can't reuse the name of the deleted instance until one week from the deletion date" in terraform_output:
        terraform_output = "instanceAlreadyExists: you can't reuse the name of the " \
                           "deleted instance until one week from the deletion date"
    elif "Terraform has compared your real infrastructure against your configuration " \
         "and found no differences, so no changes are needed" in terraform_output:
        terraform_output = "Terraform has compared your real infrastructure against your configuration " \
                           "and found no differences, so no changes are needed"
    else:
        return terraform_output
    return terraform_output


def GetKeyValueFromDict(dict_object, dict_key):
    """
    Get dict object and return a value based on provided key
    :param dict_object:  dict object
    :param dict_key:  key in the dict
    :return: return a value based on the key
    """
    logging.debug('repo: ' + str(dict_object) + ', file: ' + str(dict_key))
    value = dict_object.get(dict_key,  "None")
    logging.debug('value: ' + str(value))

    return str(value)


# def enable_logging():
#     """
#     Enable logging
#     :return: the unique task id for an execution
#     """
#     # Allocate unique task id so each api call will have a log.
#     task_id = str(randrange(100000000, 999999999))
#     # Logging information and configuration
#     log_file = '/code/logs/di_manager_' + task_id + '.log'
#     logger = logging.getLogger(task_id)
#     logger.setLevel(logging.DEBUG)
#     formatter = logging.Formatter('[%(asctime)s %(lineno)d %(levelname)s %(funcName)20s()]  %(message)s',
#                                   datefmt='%d/%m/%Y %H:%M:%S')
#     fh = logging.FileHandler(log_file)
#     sh = logging.StreamHandler(sys.stdout)
#     fh.setFormatter(formatter)
#     sh.setFormatter(formatter)
#     logger.handlers.clear()
#     if not logger.addHandler(fh):
#         logger.addHandler(fh)
#     if not logger.addHandler(sh):
#         logger.addHandler(sh)
#     logging.info('New task id allocated: ' + task_id)
#     logging.info('Log file: ' + log_file)
#     return task_id

class ThreadLogFilter(logging.Filter):
    """
    This filter only show log entries for specified thread name
    """

    def __init__(self, thread_name, *args, **kwargs):
        logging.Filter.__init__(self, *args, **kwargs)
        self.thread_name = thread_name

    def filter(self, record):
        return record.threadName == self.thread_name


def start_thread_logging(task_id):
    """
    Add a log handler to separate file for current thread
    """
    task_id = str(task_id)
    thread_name = threading.Thread.getName(threading.current_thread())
    log_file = "/code/logs/task-" + task_id + ".log"
    log_handler = logging.FileHandler(log_file)

    log_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)-15s"
        "| %(funcName)-11s"
        "| %(threadName)-11s"
        "| task_id: " + task_id +
        "| %(levelname)-5s"
        "| %(message)s")
    log_handler.setFormatter(formatter)

    log_filter = ThreadLogFilter(thread_name)
    log_handler.addFilter(log_filter)

    logger = logging.getLogger()
    logger.addHandler(log_handler)

    logging.info('Log file: ' + log_file)
    return log_handler


def stop_thread_logging(log_handler):
    # Remove thread log handler from root logger
    logging.getLogger().removeHandler(log_handler)

    # Close the thread log handler so that the lock on log file can be released
    log_handler.close()


def enable_logging():
    task_id = str(randrange(100000000, 999999999))
    log_file = '/code/logs/task_' + task_id + '.log'

    formatter = "%(asctime)-15s" \
                "| %(funcName)-11s"  \
                "| task_id: " + task_id + \
                "| %(levelname)-5s" \
                "| %(message)s"

    logging.config.dictConfig({
        'version': 1,
        'formatters': {
            'root_formatter': {
                'format': formatter
            }
        },
        'handlers': {
            'console': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'formatter': 'root_formatter'
            },
            'log_file': {
                'class': 'logging.FileHandler',
                'level': 'DEBUG',
                "encoding": "utf-8",
                'filename': log_file,
                'formatter': 'root_formatter',
            }
        },
        'loggers': {
            '': {
                'handlers': [
                    'console',
                    'log_file',
                ],
                'level': 'DEBUG',
                'propagate': True
            }
        }
    })
    logging.info('New task id allocated: ' + task_id)
    return task_id


# def enable_logging():
#
#     # Allocate unique task id so each api call will have a log.
#     task_id = str(randrange(100000000, 999999999))
#     # Logging information and configuration
#     log_file = '/code/logs/di_manager_' + task_id + '.log'
#     a_logger = logging.getLogger('')
#     a_logger.setLevel(logging.DEBUG)
#
#     formatter = logging.Formatter('[%(asctime)s %(lineno)d %(levelname)s %(funcName)20s()] %(threadName)s %(message)s',
#                                   datefmt='%d/%m/%Y %H:%M:%S')
#
#     stdout_handler = logging.StreamHandler(sys.stdout)
#     a_logger.addHandler(stdout_handler)
#     stdout_handler.setFormatter(formatter)
#
#     output_file_handler = logging.FileHandler(log_file)
#     a_logger.addHandler(output_file_handler)
#     output_file_handler.setFormatter(formatter)
#
#     return task_id



def LogRequest(api, request, exit_code, response, http_verb):
    logging.debug('api: ' + str(api) + ' ,exit_code: ' + str(exit_code) + ' ,http_verb: ' + str(http_verb))
    """
    Based on info, create an Audit table log entry representing this api request.

    :api, the class/function name in views.py calling this LogRequest
    :request, the request api payload
    :exit_code, the http exit status code (ie 200, 400 etc)
    :response, the response payload.
    :http_verb, the http verb ie GET, PUT, POST, DELETE etc
    """
    try:
        cfg_results = getdefaultvalues()
        data_infra = cfg_results.get('data_infra')
        api_url = request.get_full_path()
        api_url = 'http://' + data_infra + api_url
        user = request.user

        request_data = request.data

        def pretty_request(parsed_request):
            logging.debug('parsed_request: ' + str(parsed_request))
            headers = ''
            try:
                for header, value in parsed_request.META.items():
                    if not header.startswith('HTTP'):
                        continue
                    header = '-'.join([h.capitalize() for h in header[5:].lower().split('_')])
                    headers += '{}: {}\n'.format(header, value)

                return (
                    '{method} HTTP/1.1\n'
                    'Content-Length: {content_length}\n'
                    'Content-Type: {content_type}\n'
                    '{headers}\n\n'
                    '{body}'
                ).format(
                    method=parsed_request.method,
                    content_length=parsed_request.META['CONTENT_LENGTH'],
                    content_type=parsed_request.META['CONTENT_TYPE'],
                    headers=headers,
                    body=parsed_request.data,
                )
            except Exception as e:
                logging.error('error: ' + str(e))

        if http_verb.upper() != 'DELETE':
            logging.debug('before: ' + str(request))
            request = pretty_request(request)
            logging.debug('after: ' + str(request))

        entry = Audit_log(api=api, username=user, api_url=api_url, request=request,
                          exit_code=exit_code, response=str(response)[:10000], request_data=request_data, http_verbs=http_verb)
        entry.save()

    except Exception as e:
        logging.error('error: ' + str(e))


def success_cleanup(target_folder):
    """

    :param target_folder: target folder to be cleaned (removed)
    :return:
    """
    logging.info('Success run, let`s cleanup')
    try:
        shutil.rmtree(target_folder)
        logging.debug('target folder deleted: ' + target_folder)
    except Exception as e:
        logging.error('error: ' + str(e))


def insert_task(task_id, api_view, payload, status):
    """
    Insert a new task into the tasks_tracking table

    :param task_id:
    :param api_view:
    :param payload:
    :param status:
    :return:
    """
    try:
        logging.info('Action: ' + str(api_view))
        logging.info('Payload: ' + str(payload.data))
        try:
            logging.info('First lets make sure this task id does not exists already')
            entry_delete = tasks_tracking.objects.get(task_id=task_id)
            entry_delete.delete()
        except Exception as e:
            logging.debug('error: ' + str(e))
        logging.info('*insert into tasks_tracking values(task_id, api_view, payload, status)*')
        logging.debug(task_id)
        logging.debug(api_view)
        logging.debug(payload)
        logging.debug(payload.user.username)
        logging.debug(status)
        entry = tasks_tracking(task_id=task_id, api=api_view, username=payload.user.username, status=status)
        entry.save()
    except Exception as e:
        logging.error('error: ' + str(e))


def update_task(task_id, status, exit_status_code, exit_msg, progress_dict=None):
    """
    update the status of a task based on task_id

    :param task_id:
    :param status:
    :param exit_status_code:
    :param exit_msg:
    :param progress_dict:
    :return:
    """

    try:
        logging.info('*update tasks_tracking set api_view, payload, status where task_id = task_id*')
        entry = tasks_tracking.objects.get(task_id=task_id)
        entry.status = status
        entry.exit_status_code = exit_status_code
        entry.progress_dict = progress_dict
        entry.exit_msg = exit_msg
        entry.save()
        logging.info('task_id: ' + str(task_id) + ' Finished')
    except Exception as e:
        logging.error('error: ' + str(e))


def query_task_status(task_id):

    try:
        #logging.info('*select * from  tasks_tracking where task_id = task_id*')
        entry = tasks_tracking.objects.get(task_id=task_id)
        # logging.debug('api: ' + str(entry.api))
        # logging.debug('status: ' + str(entry.status))
        # logging.debug('exit_status_code: ' + str(entry.exit_status_code))
        # logging.debug('exit_msg: ' + str(entry.exit_msg))
        return entry.status, entry.api, entry.exit_status_code, entry.exit_msg

    except tasks_tracking.DoesNotExist:
        error = 'task_id: ' + str(task_id + ' does not exists')
        logging.error(error)
        return None, None, 400, error
    except Exception as e:
        logging.error('error: ' + str(e))
        return None, None, 500, e


def get_progress_dict(task_id, continue_if_running="no"):
    try:
        logging.info('*select progress_dict from  tasks_tracking where task_id = task_id*')
        entry = tasks_tracking.objects.get(task_id=task_id)
        logging.debug('progress_dict: ' + str(entry.progress_dict))
        logging.debug('status: ' + str(entry.status))

        if entry.status != 'Finished' and continue_if_running == 'no':
            error = 'task_id: ' + str(task_id) + ' status should be Finished to allow continuation run'
            logging.error(error)
            return None, error

        return entry.progress_dict, None

    except tasks_tracking.DoesNotExist:
        error = 'task_id: ' + str(task_id + ' does not exists')
        logging.error(error)
        return None, error
    except Exception as e:
        logging.error('error: ' + str(e))


def delete_cluster_record(cluster_name, project_id, cluster_stack):
    """
    delete a cluster from the deployment_overview table, but first copy the record to the archive table

    :param cluster_name:
    :param project_id:
    :param cluster_stack:
    :return:
    """
    try:
        logging.info('First lets move the record to the archive table')
        get_entry = deployment_overview.objects.get(cluster_name=cluster_name, project_id=project_id,
                                                    cluster_stack=cluster_stack)

        insert_entry = deployment_overview_archive(deployment_date=get_entry.deployment_date,
                                                   environment=get_entry.environment,
                                                   cluster_stack=get_entry.cluster_stack,
                                                   cluster_name=get_entry.cluster_name,
                                                   project_name=get_entry.project_name,
                                                   username=get_entry.username, region=get_entry.region,
                                                   project_id=get_entry.project_id, status=get_entry.status)
        insert_entry.save()

        logging.info('*delete target cluster from database*')
        delete_entry = deployment_overview.objects.get(cluster_name=cluster_name, project_id=project_id,
                                                       cluster_stack=cluster_stack)
        delete_entry.delete()
    except Exception as e:
        logging.error('error: ' + str(e))


def initialize_google_client(project_id):
    cfg_results = getdefaultvalues()
    gcloud_path = cfg_results.get('gcloud_path')
    command = gcloud_path + " auth activate-service-account --key-file=/root/gcp_key.json; " + \
              gcloud_path + " config set project " + project_id
    logging.debug('About to execute: ' + str(command))
    process = subprocess.run(command, capture_output=True, text=True, shell=True)


def mark_kill_task(task_id):
    """
    mark a task with status killed.
    :param task_id:
    :return:
    """

    try:
        logging.info('*mark task id: ' + str(task_id) + ' for a kill*')

        logging.debug('Lets check the current status of the task')
        current_status, query_api, query_status_code, query_error = query_task_status(task_id)
        if current_status == 'Killed':
            return 1, 'Task: ' + str(task_id) + ' was already killed.'
        if current_status != 'Running':
            return 1, query_error

        entry = tasks_tracking.objects.get(task_id=task_id)
        entry.status = 'mark_killed'
        entry.exit_status_code = 400
        entry.exit_msg = 'The task was mark killed by a user'
        entry.save()
        status = 'task_id: ' + str(task_id) + ' is now marked for a kill'
        logging.info(status)
        return 0, status
    except Exception as e:
        logging.error('error: ' + str(e))
        return 1, str(e)


def check_kill_status(task_id):
    """
    This function check the status of task, if it marked to be killed, this function will sys.exit
    :param task_id:
    :return:
    """

    logging.debug('Lets check the current status of the task')
    current_status, query_api, query_status_code, query_error = query_task_status(task_id)

    if current_status == 'mark_killed':
        status = 'Task: ' + str(task_id) + ' is now killed (by user).'
        logging.info(status)
        update_task(task_id, 'Killed', 400, status)
        sys.exit(0)

    logging.debug('task_id: ' + str(task_id) + ' is in status: ' + current_status + ' , Lets continue')


def calculate_work_time(cluster_size, nodes_so_far, execution_time):
    """
    the purpose of this function is to calculate how much time is left for a rolling operation

    :param cluster_size: total cluster size
    :param nodes_so_far: how many nodes left
    :param execution_time: in seconds
    :return:
    """
    def convert(seconds):
        seconds = seconds % (24 * 3600)
        hour = seconds // 3600
        seconds %= 3600
        minutes = seconds // 60
        seconds %= 60

        return "%d:%02d:%02d" % (hour, minutes, seconds)

    avg_node = execution_time/nodes_so_far
    avg_node_formatted = str(convert(round(avg_node)))
    logging.info('Avg time for each node so far in seconds: ' + avg_node_formatted)

    # time left
    nodes_left = cluster_size-nodes_so_far
    time_left = avg_node*nodes_left
    logging.info('estimated time in seconds: ' + str(convert(round(time_left))))

    estimated_time = datetime.now(pytz.timezone('Asia/Jerusalem')) + timedelta(seconds=time_left)
    # estimated_time += timedelta(seconds=time_left)
    logging.info('This is the estimated time: ' + str(estimated_time))
    return estimated_time, avg_node_formatted


# @retry(stop_max_delay=10000, stop_max_attempt_number=10)
# def port_check(target_host, target_port):
#     """
#     check if port is open (True) or close (false).
#     :param target_host:
#     :param target_port:
#     :return:
#     """
#     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     try:
#         s.settimeout(10)
#         s.connect((target_host, int(target_port)))
#         s.settimeout(None)
#         s.shutdown(2)
#         return True
#     except:
#         return False

async def port_check(host, port, duration=30, delay=2):
    """
    Repeatedly try if a port on a host is open until duration seconds passed
    Parameters
    ----------
    host : str
        host ip address or hostname
    port : int
        port number
    duration : int, optional
        Total duration in seconds to wait, by default 10
    delay : int, optional
        delay in seconds between each try, by default 2

    Returns
    -------
    awaitable bool
    """
    tmax = time.time() + duration
    while time.time() < tmax:
        try:
            _reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=5)
            writer.close()
            await writer.wait_closed()
            return True
        except:
            if delay:
                await asyncio.sleep(delay)
    return False


def is_valid_IP_Address(ip_str):
    """
    Returns True if given string is a
    valid IP Address, else returns False
    """
    result = True
    try:
        socket.inet_aton(ip_str)
    except socket.error:
        result = False
    return result


def write_payload_to_json(input_dict, target_path):
    """
    Write dict as json file

    :param input_dict:
    :param target_path:
    :return:
    """

    try:
        target_file = os.path.join(target_path,"payload.json")
        with open(target_file, 'w') as fout:
            json_dumps_str = json.dumps(input_dict, indent=4)
            print(json_dumps_str, file=fout)
    except Exception as e:
        logging.error('error: ' + str(e))


