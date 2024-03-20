from di_app_manager.app_code.validation import *
from django.utils import timezone


@retry(stop=(stop_after_attempt(10)))
def query_clusters_repo(request):
    """
    based on payload query the database deployment_overview table

    :param request:
    :return:
    """
    try:
        logging.info('Lets query the deployment_overview table')
        cfg_results = getdefaultvalues()
        limiting_queryset = int(cfg_results.get('limiting_queryset'))
        request_data = request.data
        cluster_stack = GetKeyValueFromDict(request_data, 'cluster_stack')
        project_id = GetKeyValueFromDict(request_data, 'project_id')
        region = GetKeyValueFromDict(request_data, 'region')
        cluster_name = GetKeyValueFromDict(request_data, 'cluster_name')
        username = GetKeyValueFromDict(request_data, 'username')
        project_name = get_project_name_from_id(project_id)
        environment = GetKeyValueFromDict(request_data, 'environment')
        get_entry = None

        # if environment is queried check if value is valid
        if environment != 'None':
            environment = environment.lower()
            if not environment == 'dev' and not environment == 'dr' and not environment == 'prod':
                return {'error': 'Environment types can be only: prod, dr or dev'}

        # if environment is queried check if value is valid
        if cluster_stack != 'None':
            cluster_stack = cluster_stack.lower()
            if not cluster_stack == 'kafka' and not cluster_stack == 'postgres' and not cluster_stack == 'elastic' \
                    and not cluster_stack == 'aerospike':
                return {'error': 'Environment types can be only: kafka/postgres/elastic/aerospike'}

        valid_key_list = ['environment', 'deployment_date', 'cluster_stack', 'unique_name',
                          'project_name', 'username', 'region', 'project_id', 'status', 'cluster_name']
        entry_list = list(request_data.keys())

        # Let`s make sure all search keys are valid
        if not set(entry_list).issubset(set(valid_key_list)):
            return {'error': 'This is the only valid search key list: ' + str(valid_key_list)}
        logging.info('Found the following search keys: ' + str(entry_list))

        # Building dynamic code to execute a dynamic query
        code = "deployment_overview.objects.filter("
        for i in entry_list:
            code = code + "" + i + "=" + i + ","
        code = code[:-1] + ")[:limiting_queryset]"
        logging.info('About to execute: ' + code)
        get_entry = eval(code)

        query_result = []
        for entry in get_entry:
            entry_result = {'deployment_date': entry.deployment_date, 'environment': entry.environment,
                            'cluster_stack': entry.cluster_stack, 'cluster_name': entry.cluster_name,
                            'project_name': entry.project_name, 'username': entry.username,
                            'region': entry.region, 'project_id': entry.project_id, 'status': entry.status}
            query_result.append(entry_result)

        logging.info('query_result: ' + str(query_result))

        return {'query_result': query_result}
    except Exception as e:
        logging.error('error: ' + str(e))


@retry(stop=(stop_after_attempt(10)))
def query_target_task(task_id):
    """
    query specific task

    :param task_id:
    :return:
    """
    try:
        logging.info('Lets query task_id: ' + str(task_id))
        finish_long_running_tasks()

        get_entry = tasks_tracking.objects.filter(task_id=task_id)

        query_result = []
        for entry in get_entry:
            entry_result = {'task_id': entry.task_id, 'entry_date': entry.entry_date,
                            'api': entry.api, 'status': entry.status,
                            'exit_status_code': entry.exit_status_code, 'exit_msg': entry.exit_msg,
                            'username': entry.username}
            query_result.append(entry_result)

        logging.info('query_result: ' + str(query_result))

        return {'query_result': query_result}
    except Exception as e:
        logging.error('error: ' + str(e))


@retry(stop=(stop_after_attempt(10)))
def query_running_tasks():
    """
    query to return all running tasks

    :return:
    """
    try:
        logging.info('Lets what tasks are running')
        finish_long_running_tasks()

        time_threshold = datetime.now(tz=timezone.utc) - timedelta(days=1)
        get_entry = tasks_tracking.objects.filter(status__in=['Running', 'mark_killed'], entry_date__gt=time_threshold)

        query_result = []
        for entry in get_entry:
            entry_result = {'task_id': entry.task_id, 'entry_date': entry.entry_date,
                            'api': entry.api, 'status': entry.status,
                            'exit_status_code': entry.exit_status_code, 'exit_msg': entry.exit_msg,
                            'username': entry.username}
            query_result.append(entry_result)

        logging.info('query_result: ' + str(query_result))

        return {'query_result': query_result}
    except Exception as e:
        logging.error('error: ' + str(e))


@retry(stop=(stop_after_attempt(10)))
def query_tasks_repo(request):
    """
    based on payload query the database tasks_tracking table

    :param request:
    :return:
    """
    try:
        logging.info('Lets query the tasks_tracking table')
        cfg_results = getdefaultvalues()
        limiting_queryset = int(cfg_results.get('limiting_queryset'))
        request_data = request.data
        api = GetKeyValueFromDict(request_data, 'api')
        status = GetKeyValueFromDict(request_data, 'status')
        exit_status_code = GetKeyValueFromDict(request_data, 'exit_status_code')
        username = GetKeyValueFromDict(request_data, 'username')
        get_entry = None

        valid_key_list = ['api', 'status', 'username', 'exit_status_code']
        entry_list = list(request_data.keys())

        # Let`s make sure all search keys are valid
        if not set(entry_list).issubset(set(valid_key_list)):
            return {'error': 'This is the only valid search key list: ' + str(valid_key_list)}
        logging.info('Found the following search keys: ' + str(entry_list))

        # Building dynamic code to execute a dynamic query
        code = "tasks_tracking.objects.filter("
        for i in entry_list:
            code = code + "" + i + "=" + i + ","
        code = code[:-1] + ")[:limiting_queryset]"
        logging.info('About to execute: ' + code)
        get_entry = eval(code)

        query_result = []
        for entry in get_entry:
            entry_result = {'task_id': entry.task_id, 'entry_date': entry.entry_date,
                            'api': entry.api, 'status': entry.status,
                            'exit_status_code': entry.exit_status_code, 'exit_msg': entry.exit_msg,
                            'username': entry.username}
            query_result.append(entry_result)

        logging.info('query_result: ' + str(query_result))

        return {'query_result': query_result}
    except Exception as e:
        logging.error('error: ' + str(e))


@retry(stop=(stop_after_attempt(10)))
def query_audit_log(request):
    """
    based on payload query the database Audit_log table

    :param request:
    :return:
    """
    try:
        logging.info('Lets query the Audit_log table')
        limiting_queryset = int(7)
        request_data = request.data
        api = GetKeyValueFromDict(request_data, 'api')
        username = GetKeyValueFromDict(request_data, 'username')
        http_verbs = (GetKeyValueFromDict(request_data, 'http_verbs')).lower()
        exit_code = GetKeyValueFromDict(request_data, 'exit_code')
        get_entry = None

        valid_key_list = ['api', 'http_verbs', 'username', 'exit_code']
        entry_list = list(request_data.keys())

        # Let`s make sure all search keys are valid
        if not set(entry_list).issubset(set(valid_key_list)):
            return {'error': 'This is the only valid search key list: ' + str(valid_key_list)}
        logging.info('Found the following search keys: ' + str(entry_list))

        # Building dynamic code to execute a dynamic query
        code = "Audit_log.objects.filter("
        for i in entry_list:
            code = code + "" + i + "=" + i + ","
        code = code[:-1] + ")[:limiting_queryset]"
        logging.info('About to execute: ' + code)
        get_entry = eval(code)

        query_result = []
        for entry in get_entry:
            entry_result = {'entry_date': entry.entry_date, 'api': entry.api,
                            'username': entry.username, 'api_url': entry.api_url,
                            'http_verbs': entry.http_verbs, 'exit_code': entry.exit_code}
            query_result.append(entry_result)

        logging.info('query_result: ' + str(query_result))

        return {'query_result': query_result}
    except Exception as e:
        logging.error('error: ' + str(e))


@retry(stop=(stop_after_attempt(10)))
def finish_long_running_tasks():
    """
    Mark as Finished any task that is more than 24 hours running.
    Also delete any finished task older than 10 days

    :return:
    """
    try:
        logging.info('delete any finished task older than 10 days.')

        time_threshold = datetime.now(tz=timezone.utc) - timedelta(days=10)
        get_entry = tasks_tracking.objects.filter(status='Finished', entry_date__lt=time_threshold).delete()

        logging.info('Mark as Finished any task that is more than 24 hours running.')

        time_threshold = datetime.now(tz=timezone.utc) - timedelta(days=1)
        get_entry = tasks_tracking.objects.filter(status='Running', entry_date__lt=time_threshold)\
            .update(status='Finished')

    except Exception as e:
        logging.error('error: ' + str(e))