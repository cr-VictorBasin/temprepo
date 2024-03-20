from di_app_manager.app_code.cluster_deployment import *
from di_app_manager.app_code.validation import *
from di_app_manager.app_code.git_procs import *
import ast


def modify_terraform_variables_pg(work_dir, payload_info):
    """
    Break the request data (the Json provided by user post request) into variables,
    verify that the provided data is valid
    Create the new instance code based on the template.
    replace the Terraform variables with the payload variables.
    :param work_dir: the temp directory create by gitcheckout, with the project code.
    :param payload_info: the request.data, the JSON the user provided.
    :return:
    exit_code:  0 - all seems good no errors, 1 - something failed in verifications or code error
    error: The error text of any
    """

    try:
        # Pull default values from cfg file
        cfg_results = getdefaultvalues()
        var_file = cfg_results.get('var_file')
        supported_versions = cfg_results.get('supported_versions')
        disk_size_limit_gb = cfg_results.get('disk_size_limit_gb')

        # Break the request data (payload) into variables.
        project_id = GetKeyValueFromDict(payload_info, 'project_id')
        project_name = get_project_name_from_id(project_id)
        instance_name = GetKeyValueFromDict(payload_info, 'instance_name')
        region = GetKeyValueFromDict(payload_info, 'region')
        env_type = GetKeyValueFromDict(payload_info, 'env_type')
        instance_type = GetKeyValueFromDict(payload_info, 'instance_type')
        database_version = GetKeyValueFromDict(payload_info, 'database_version')
        disk_size = GetKeyValueFromDict(payload_info, 'disk_size')
        availability_type = GetKeyValueFromDict(payload_info, 'availability_type')
        private_network = GetKeyValueFromDict(payload_info, 'private_network')
        operation_type = GetKeyValueFromDict(payload_info, 'operation_type')
        backup_retention_days = GetKeyValueFromDict(payload_info, 'backup_retention_days')
        backup_point_in_time_recovery = GetKeyValueFromDict(payload_info, 'backup_point_in_time_recovery')
        backup_start_time = GetKeyValueFromDict(payload_info, 'backup_start_time')
        database_flags = GetKeyValueFromDict(payload_info, 'database_flags')
        disk_autoresize = GetKeyValueFromDict(payload_info, 'disk_autoresize')

        # Verify PG version is in the supported list, else halt and exit
        verify_exit_code = verify_pg_version(database_version)
        if verify_exit_code == 0:
            if database_version == str(9.6):
                database_version = 'POSTGRES_9_6'
            else:
                database_version = 'POSTGRES_' + str(database_version)
        else:
            return 1, 'unsupported PG version, supported list: ' + str(supported_versions)

        # Verify availability_type is valid
        if not availability_type == 'ZONAL' and not availability_type == 'REGIONAL':
            return 1, 'availability type supports only the following options:, ' \
                      'high availability (REGIONAL) or single zone (ZONAL)'

        # Validate if project already exists
        return_code, return_msg = check_project_exists(project_id)
        if return_code == 0 and return_msg == 'not exists':
            return 1, 'GCP Project: ' + str(project_id) + ' does not exists'

        # Verify env_type is valid (dev dr or prod)
        env_type = env_type.lower()
        if not env_type == 'dev' and not env_type == 'dr' and not env_type == 'prod':
            return 1, 'Environment types: prod, dr or dev'

        # Verify disk_size is valid positive number
        if type(disk_size) == int:
            if disk_size < 1 or disk_size > int(disk_size_limit_gb):
                return 1, 'disk_size must be a positive number between 1 to 10000 (GB)'
        elif disk_size.isnumeric():
            disk_size = int(disk_size)
            if disk_size < 1 or disk_size > int(disk_size_limit_gb):
                return 1, 'disk_size must be a positive number between 1 to 10000 (GB)'
        else:
            return 1, 'disk_size must be a positive number between 1 to 10000 (GB)'

        # Verify instance_name format is valid
        # Instance name must be Composed of lowercase letters, numbers, and hyphens; must start with a letter.
        import string
        allowed = string.ascii_letters + string.digits + '-'

        def check_naive(my_string):
            return all(c in allowed for c in my_string) and my_string[0].isalpha()
        if not check_naive(instance_name):
            return 1, 'Instance name must be Composed of lowercase letters, numbers,' \
                      ' and hyphens(-); must start with a letter.'

        # Check operation_type, do we want a new environment created or scale/change existing one
        logging.debug('Let` verify operation_type: ' + operation_type)
        if not operation_type == 'new' and not operation_type == 'scale':
            return 1, 'operation_type should be new or scale, new to create new environment,' \
                      ' scale to alter existing one'

        # Adjust directory locations
        orig_dir = os.getcwd()
        os.chdir(work_dir)
        current_dir = os.getcwd()

        # prepare template and target dir paths
        template_path = os.path.join(current_dir, 'projects', 'template/')
        logging.debug('template_path: ' + str(template_path))
        target_path = os.path.join(current_dir, 'projects', project_name, instance_name)
        logging.debug('target_path: ' + str(target_path))

        if os.path.isdir(target_path) and operation_type == 'new':
            return 1, 'found an existing environment code, you asked to create a fresh environment (operation_type=new)'
        elif not os.path.isdir(target_path) and operation_type == 'scale':
            return 1, 'could not find existing environment code, ' \
                      'and you asked to alter/scale existing environment (operation_type=scale)'

        # if operation type scale delete target directory
        if operation_type == 'scale':
            shutil.rmtree(target_path)

        # copy from template dir to target  new project/instance directory.
        logging.info('Let`s copy a new instance code from template')
        shutil.copytree(template_path, target_path)
        logging.info('Copy files from ' + str(template_path) + ' To ' + str(target_path))
        vars_file_full_path = os.path.join(target_path, var_file)

        # Verify Region provided is valid
        exit_verify_region = verify_region(region)
        if exit_verify_region == 1:
            logging.error('Provided GCP Region ( ' + region + ' ) doesnt exists')
            return 1, 'Provided GCP Region ( ' + region + ' ) doesnt exists'

        # Set database_flags
        database_flags_string = ''
        key_val = ''
        logging.debug(database_flags)

        if database_flags != 'None':
            logging.debug(database_flags)
            logging.debug(type(database_flags))
            flags = ast.literal_eval(database_flags)
            j = 0
            for i in flags:
                if j == 0:
                    key_val = ("{\"" + i['name'] + '\": ' + i['value'] + ' ')
                else:
                    key_val = ("\"" + i['name'] + '\": ' + i['value'] + ' ')

                j += 1
                string = key_val + ','
                #print(database_flags_string)
                database_flags_string += string

            database_flags_string = database_flags_string[:-1]
            database_flags_string = database_flags_string + '}'


        # Build the payload dict (after verification and interpolation)
        changes_dict = {'<region>': region, '<env_type>': env_type, '<instance_name>': instance_name,
                        '<instance_type>': instance_type, '<project_id>': project_id,
                        '<database_version>': database_version, '!disk_size': str(disk_size),
                        '<availability_type>': availability_type, '<private_network>': private_network,
                        '!backup_retention_days': str(backup_retention_days),
                        '<backup_point_in_time_recovery>': backup_point_in_time_recovery,
                        '<backup_start_time>': backup_start_time,
                        '<disk_autoresize>': disk_autoresize,
                        '!database_flags': database_flags_string}
        logging.debug(changes_dict)

        def replace_all(text, dic):
            for i, j in dic.items():
                text = text.replace(i, j)
            return text

        # Read the new instance variable file and replace the values with the payload values
        with open(vars_file_full_path) as f:
            var_text = f.read()
            var_text = replace_all(var_text, changes_dict)
        with open(vars_file_full_path, "w") as f:
            f.write(var_text)
        os.chdir(orig_dir)
        logging.debug('finished executing modify_terraform_variables_pg')
        return 0, None
    except Exception as error:
        logging.error(str(error))
        return 1, str(error)


def cloudsql_create_db(project_id, instance_name, database_name):
    """
    Create a new database under existing cloudSQL instance
    :param project_id:
    :param project_id:
    :param instance_name:
    :param database_name:
    :return:
    """

    logging.info('Creating a new database: ' + str(database_name + ' under CloudSQL instance ' + str(instance_name)))
    cfg_results = getdefaultvalues()
    gcloud_path = cfg_results.get('gcloud_path')

    # Validate if project already exists
    return_code, return_msg = check_project_exists(project_id)
    if return_code == 0 and return_msg == 'not exists':
        error_msg = 'GCP Project: ' + str(project_id) + ' does not exists'
        logging.error(error_msg)
        return 1, error_msg

    # Validate if database_name is compliant
    if len(database_name) > 60:
        error_msg = 'database_name should be less or equal to 60 characters'
        logging.error(error_msg)
        return 1, error_msg

    try:
        command = gcloud_path + ' config set project ' + project_id + '; ' + gcloud_path + ' sql databases create '\
                  + database_name + ' --instance=' + instance_name
        logging.debug('About to execute: ' + str(command))
        subprocess.check_output(command, shell=True)
    except subprocess.CalledProcessError as grepexc:
        logging.error("error code", grepexc.returncode, grepexc.output)
        return 1, 'error code ' + str(grepexc.returncode) + ' ' + str(grepexc.output)
    return 0, 'Database: ' + database_name + ' created at: ' + str(instance_name)


def create_new_database(task_id, payload):
    """
    Based on provided payload create a new cloud sql database under an existing instance
    :param task_id:
    :param payload:
    :return:
    """
    
    logging.info('This is my task_id: ' + str(task_id))
    project_id = GetKeyValueFromDict(payload.data, 'project_id')
    instance_name = GetKeyValueFromDict(payload.data, 'instance_name')
    database_name = GetKeyValueFromDict(payload.data, 'database_name')
    target_mail_address = GetKeyValueFromDict(payload.data, 'target_mail_address')
    logging.info('Creating a new database: ' + str(database_name + ' under CloudSQL instance ' + str(instance_name)))

    exit_code, status = cloudsql_create_db(project_id, instance_name, database_name)
    logging.debug('cloudsql_create_db exit_code: ' + str(exit_code))
    logging.debug('cloudsql_create_db status: ' + str(status))

    if exit_code == 0:
        status_code = 200
    else:
        status_code = 500
    mail_dict = {"subject": 'DI Manager: ' + str(status),
                 "body": str(payload.data) + os.linesep + os.linesep + 'Status: ' + str(status)}
    LogRequest('create_new_database', payload, status_code, status, 'post')
    LogDeployment(payload, 'online', 'postgres')
    sendemail(target_mail_address, mail_dict)
    logging.info('status: ' + status)
    update_task(task_id, 'Finished', status_code, status)
    return status_code


def cloudsql_create_user(project_id, instance_name, username, password):
    """
    Create a new database under existing cloudSQL instance
    :param password:
    :param username:
    :param project_id:
    :param instance_name:
    :return:
    """

    logging.info('Creating a new user: ' + str(username + ' under CloudSQL instance ' + str(instance_name)))
    cfg_results = getdefaultvalues()
    gcloud_path = cfg_results.get('gcloud_path')

    # Validate if project already exists
    return_code, return_msg = check_project_exists(project_id)
    if return_code == 0 and return_msg == 'not exists':
        error_msg = 'GCP Project: ' + str(project_id) + ' does not exists'
        logging.error(error_msg)
        return 1, error_msg

    try:
        command = gcloud_path + ' config set project ' + project_id + '; ' + gcloud_path + ' sql users create ' \
                  + username + ' --password=' + password + ' --instance=' + instance_name
        logging.debug('About to execute: ' + str(command))
        subprocess.check_output(command, shell=True)
    except subprocess.CalledProcessError as grepexc:
        logging.error("error code", grepexc.returncode, grepexc.output)
        return 1, 'error code ' + str(grepexc.returncode) + ' ' + str(grepexc.output)
    return 0, 'Username: ' + username + ' created at: ' + str(instance_name)


def create_new_user(task_id, payload):
    """
    Based on provided payload create a new cloud sql database under an existing instance
    :param payload:
    :param task_id:
    :return:
    """
    
    logging.info('This is my task_id: ' + str(task_id))
    project_id = GetKeyValueFromDict(payload.data, 'project_id')
    username = GetKeyValueFromDict(payload.data, 'username')
    password = GetKeyValueFromDict(payload.data, 'password')
    instance_name = GetKeyValueFromDict(payload.data, 'instance_name')
    target_mail_address = GetKeyValueFromDict(payload.data, 'target_mail_address')
    logging.info('Creating a new username: ' + str(username) + ' under CloudSQL instance ' + str(instance_name))

    exit_code, status = cloudsql_create_user(project_id, instance_name, username, password)
    logging.debug('cloudsql_create_user exit_code: ' + str(exit_code))
    logging.debug('cloudsql_create_user status: ' + str(status))

    if exit_code == 0:
        status_code = 200
    else:
        status_code = 500
    mail_dict = {"subject": 'DI Manager: ' + str(status),
                 "body": str(payload.data) + os.linesep + os.linesep +
                 "Status: " + str(status)}
    LogRequest('cloudsql_create_user', payload, status_code, status, 'post')
    LogDeployment(payload, 'online', 'postgres')
    sendemail(target_mail_address, mail_dict, 'username: ' + str(username) + ' password: ' + str(password))
    logging.info('status: ' + status)
    update_task(task_id, 'Finished', status_code, status)
    return status_code


def deploy_new_pg_instance(task_id, payload):
    """
    Main Deployment code for postgres cloud sql instance
    :param task_id:
    :param payload:
    :return:
    """
    
    logging.info('This is my task_id: ' + str(task_id))
    target_mail_address = GetKeyValueFromDict(payload.data, 'target_mail_address')
    project_id = GetKeyValueFromDict(payload.data, 'project_id')
    project_name = get_project_name_from_id(project_id)
    instance_name = GetKeyValueFromDict(payload.data, 'instance_name')
    operation_type = GetKeyValueFromDict(payload.data, 'operation_type')
    git_version = GetKeyValueFromDict(payload.data, 'postgresql_git_version')
    gitcheckout_exit_code, temp_folder = gitcheckout(git_version, 'pg_git_path')
    project_dir = os.path.join(temp_folder, 'projects', project_name, instance_name)
    if gitcheckout_exit_code == 0:
        modify_exit_code, modify_error = modify_terraform_variables_pg(temp_folder, payload.data)
        if modify_exit_code == 0 and modify_error is None:
            terraform_return_code, terraform_error = terraform_action(project_dir, 'apply', terraform_lock='no')
            if terraform_return_code == 0:
                commit_exit_code, commit_error = \
                    gitcommitpush(temp_folder, 'DI-Manager: NEW PG instance created at: '
                                  + project_name + '@' + instance_name)
                if commit_exit_code == 0:
                    if operation_type == 'scale':
                        status = str('Successfully Scaled/Altered PG instance '
                                     + project_name + '@' + instance_name)
                    else:
                        status = str('Successfully deployed new PG instance ' + project_name + '@'
                                     + instance_name)
                    status_code = 200
                else:
                    status = commit_error
                    status_code = 202
            else:
                if terraform_error is None:
                    terraform_error = 'Unable to create/apply terraform code'
                status = str(terraform_error)
                status_code = 202
        else:
            if modify_error is None:
                modify_error = 'Unable to create/apply terraform code'
            status = modify_error
            status_code = 202
    else:
        status = 'Unable to checkout code from git'
        status_code = 202
    mail_dict = {"subject": 'DI Manager: ' + str(status),
                 "body": str(payload.data) + os.linesep + os.linesep +
                 "Status: " + str(status)}
    LogRequest('DeployNewPgInstance', payload, status_code, status, 'post')
    sendemail(target_mail_address, mail_dict)
    if status_code == 200:
        success_cleanup(temp_folder)
    update_task(str(task_id), 'Finished', status_code, str(status))
    return status_code


def get_pg_instance_details(payload):
    """
    from request payload get instance information
    :return: return the ip of the instance
    """
    try:
        project_id = GetKeyValueFromDict(payload.data, 'project_id')
        target_mail_address = GetKeyValueFromDict(payload.data, 'target_mail_address')
        instance_name = GetKeyValueFromDict(payload.data, 'instance_name')
        region = GetKeyValueFromDict(payload.data, 'region')
        database_name = GetKeyValueFromDict(payload.data, 'database_name')
        username = GetKeyValueFromDict(payload.data, 'username')
        password = GetKeyValueFromDict(payload.data, 'password')
        connection_string = project_id + ":" + region + ":" + instance_name
        cfg_results = getdefaultvalues()
        gcloud_path = cfg_results.get('gcloud_path')
        command = gcloud_path + " auth activate-service-account --key-file=/root/gcp_key.json; " + \
                  gcloud_path + " config set project " + project_id + "; " + \
                  gcloud_path + " sql instances list  |grep " + instance_name + " |awk '{print $6 }'"
        logging.debug('About to execute: ' + str(command))
        process = subprocess.run(command, capture_output=True, text=True, shell=True)
        output = process.stdout
        if output:
            logging.debug('output: ' + str(output))
        output_error = process.stderr
        if output_error:
            logging.debug('output_error: ' + str(output_error))
        if process.returncode == 0:
            mail_dict = {"subject": 'DS Manager, PG instance: ' + str(instance_name) + ' created at ' + project_id,
                         "body": str(payload.data) + os.linesep + os.linesep +
                         "project_id: " + project_id + os.linesep +
                         "connection_string: " + connection_string + os.linesep +
                         "ip address: " + str(output) + os.linesep +
                         "instance_name: " + str(instance_name) + os.linesep +
                         "Database created: " + database_name + os.linesep +
                         "Username created: " + username + os.linesep +
                         "Password will be sent in a second email"}
            sendemail(target_mail_address, mail_dict)
            mail_dict = {"subject": 'DS Manager, PG instance: ' + str(instance_name) + ' created at ' + project_id,
                         "body": "Password: " + str(password)}
            sendemail(target_mail_address, mail_dict)
            return output, connection_string
        else:
            raise Exception(output_error)
    except Exception as e:
        logging.error(str(e))
        sys.exit(1)


def fully_create_pg_instance(task_id, payload):
    """
    Deploy New Cloud SQL Pg instance ,then create new database and a new user
    :param task_id:
    :param payload:
    :return:
    """
    try:
        status_code = deploy_new_pg_instance(task_id, payload)
        if status_code == 200:
            create_new_database(task_id, payload)
            if status_code == 200:
                create_new_user(task_id, payload)
                if status_code == 200:
                    get_pg_instance_details(payload)
    except Exception as e:
        logging.error(str(e))
        sys.exit(1)
