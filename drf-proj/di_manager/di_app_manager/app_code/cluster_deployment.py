from di_app_manager.app_code.validation import *
from di_app_manager.app_code.git_procs import *
import string


def modify_terraform_variables_cluster(work_dir, payload_info, deployment_type, cluster_stack):
    """
    Break the request data (the Json provided by user post request) into variables,
    verify that the provided data is valid
    Create the new instance code based on the template.
    replace the Terraform variables with the payload variables.
    :param work_dir: the temp directory create by gitcheckout, with the project code.
    :param payload_info: the request. data, the JSON the user provided.
    :param cluster_stack: the tech stack, ie elastic or kafka
    :return:
    exit_code:  0 - all seems good no errors, 1 - something failed in verifications or code error
    error: The error text of any
    """

    try:
        # Pull default values from cfg file
        cfg_results = getdefaultvalues()
        var_file = cfg_results.get('var_file')
        valid_key_list = cfg_results.get(cluster_stack + '_valid_key_list')
        valid_key_list = valid_key_list.replace(" ", "")
        valid_key_list = valid_key_list.split(",")
        logging.debug('valid_key_list: ' + str(valid_key_list))
        supported_versions = cfg_results.get(cluster_stack + '_supported_versions')
        supported_versions = supported_versions.replace(" ", "")
        supported_versions = supported_versions.split(",")
        logging.debug('supported ' + cluster_stack + ' versions: ' + str(supported_versions))
        disk_size_limit_gb = cfg_results.get('disk_size_limit_gb')
        host_projects = cfg_results.get(cluster_stack + '_host_projects')
        host_projects = json.loads(host_projects)
        cluster_type = GetKeyValueFromDict(payload_info, 'cluster_type')
        logging.debug('cluster_type: ' + str(cluster_type))
        cluster_type_dict = cfg_results.get(cluster_stack + '_cluster_type_dict')
        cluster_type_dict = json.loads(cluster_type_dict)
        cluster_type_list = list(cluster_type_dict['small'].keys())
        logging.debug('cluster_type_dict: ' + str(cluster_type_dict))
        logging.debug('cluster_type_list: ' + str(cluster_type_list))

        # Break the request data (payload) into variables.
        project_id = GetKeyValueFromDict(payload_info, 'project_id')
        project_name = get_project_name_from_id(project_id)
        cluster_name = GetKeyValueFromDict(payload_info, 'cluster_name')
        env_type = GetKeyValueFromDict(payload_info, 'env_type')
        cluster_version = GetKeyValueFromDict(payload_info, cluster_stack + '_version')
        logging.debug('cluster_version is: ' + cluster_version)
        # Verifying that cluster version was added
        if cluster_version == "None":
            cluster_version = cfg_results.get(cluster_stack + '_version')
            logging.debug(cluster_stack + ' Cluster version was not selected setting default '
                          + cluster_version)

        # Create the Action dict object
        results_dict = {}
        for i in valid_key_list:
            code = "results_dict['" + i + "'] = str(payload_info.get('" + i + "'))"
            logging.debug(code)
            exec(code)
        results_dict = {k: v for k, v in results_dict.items() if v != 'None'}
        logging.debug('results_dict: ' + str(results_dict))
        for item in valid_key_list:
            if item not in results_dict.keys():
                if item in cluster_type_list:
                    code = "results_dict['" + item + "'] = cluster_type_dict['" + cluster_type + "']['" + item + "']"
                    logging.debug(code)
                    exec(code)
                elif item == 'host_project':
                    code = "results_dict['" + item + "'] = host_projects['" + env_type + "']"
                    logging.debug(code)
                    exec(code)
                elif item not in results_dict:
                    code = "results_dict['" + item + "'] = cfg_results.get('" + item + "')"
                    logging.debug(code)
                    exec(code)
        logging.debug(type(results_dict))
        logging.debug('This is the full payload: ' + str(results_dict))

        # validate parameters
        if cluster_stack != 'elastic':
            return_code, return_msg = validity_parameters(results_dict, cluster_stack)
            if return_code != 0:
                return 1, return_msg

        # Validate if project exists
        return_code, return_msg = check_project_exists(project_id)
        if return_code == 0 and return_msg == 'not exists':
            return 1, 'GCP Project: ' + str(project_id) + ' does not exists'

        # Verify env_type is valid (dev dr or prod)
        env_type = env_type.lower()
        if not env_type == 'dev' and not env_type == 'dr' and not env_type == 'prod' and not env_type == 'csdev':
            return 1, 'Environment types: prod, dr, dev or csdev'

        # Verify cluster_name format is valid
        # Instance name must be Composed of lowercase letters, numbers, and hyphens; must start with a letter.
        allowed = string.ascii_letters + string.digits + '-'

        # Verify cluster_version is valid, version must be within supported_versions
        logging.debug("lets verify that version " + cluster_version + ' is supported')
        logging.debug('supported ' + cluster_stack + ' versions: ' + str(supported_versions))
        if cluster_version not in supported_versions:
            logging.error(cluster_stack + " version " + cluster_version + ' is not supported')
            return 1, cluster_stack + '_cluster_version: ' + cluster_version + ' is not supported'

        def check_naive(my_string):
            return all(c in allowed for c in my_string) and my_string[0].isalpha()
        if not check_naive(cluster_name):
            return 1, 'Instance name must be Composed of lowercase letters, numbers,' \
                      ' and hyphens(-); must start with a letter.'

        # Adjust directory locations
        orig_dir = os.getcwd()
        os.chdir(work_dir)
        current_dir = os.getcwd()

        # prepare template and target dir paths
        template_path = os.path.join(current_dir, 'projects', 'template/')
        logging.debug('template_path: ' + str(template_path))
        target_path = os.path.join(current_dir, 'projects', project_name, cluster_name)
        logging.debug('target_path: ' + str(target_path))

        # copy from template dir to target  new project/instance directory.
        logging.info('Let`s copy a new instance code from template')
        shutil.copytree(template_path, target_path)
        logging.info('Copy files from ' + str(template_path) + ' To ' + str(target_path))
        vars_file_full_path = os.path.join(target_path, var_file)

        # Adjust new Tf code to specific Git version or branch
        git_version = results_dict[cluster_stack + '_git_version']
        logging.info('Git Version applied: ' + str(git_version))
        # latest means work against master branch (ie latest) so no need to make any changes
        if git_version != 'latest':
            main_file_full_path = os.path.join(target_path, 'main.tf')
            logging.debug('main file location: ' + str(main_file_full_path))
            fin = open(main_file_full_path, "rt")
            data = fin.read()
            data = data.replace('ref=master', 'ref=' + str(git_version))
            fin.close()
            fin = open(main_file_full_path, "wt")
            fin.write(data)
            fin.close()

        # Verify Region provided is valid
        region = results_dict['region']
        exit_verify_region = verify_region(region)
        if exit_verify_region == 1:
            logging.error('Provided GCP Region ( ' + region + ' ) is not supported')
            return 1, 'Provided GCP Region ( ' + region + ' ) is not supported'

        changes_dict = {}
        for key, value in results_dict.items():
            try:
                value = str(value)
                code = "changes_dict['!" + key + "'] = str('" + value + "')"
                exec(code)
            except Exception as error:
                logging.error(str(error))
                raise Exception(str(error) + ' at: ' + str(code))
        logging.debug(changes_dict)

        def replace_all(text, dic):
            for x, j in dic.items():
                j = str(j)
                text = text.replace(x, j)
            return text

        # Read the new instance variable file and replace the values with the payload values
        with open(vars_file_full_path) as f:
            var_text = f.read()
            var_text = replace_all(var_text, changes_dict)
        with open(vars_file_full_path, "w") as f:
            f.write(var_text)
        os.chdir(orig_dir)
        logging.debug('finished executing modify_terraform_variables_cluster')
        return 0, None
    except Exception as error:
        logging.error(str(error))
        return 1, str(error)


def create_new_cluster(task_id, payload, cluster_stack):
    """
    The code for the create new cluster view

    :param task_id:
    :param payload: the user provided JSON with all the info needed to create a new cluster
    :param cluster_stack: elastic or kafka
    :return:
    """
    try:
        logging.info('This is my task_id: ' + str(task_id))
        target_mail_address = GetKeyValueFromDict(payload.data, 'target_mail_address')
        project_id = GetKeyValueFromDict(payload.data, 'project_id')
        project_name = get_project_name_from_id(project_id)
        cluster_name = GetKeyValueFromDict(payload.data, 'cluster_name')
        deployment_type = GetKeyValueFromDict(payload.data, 'cluster_type')
        logging.debug(type(deployment_type))
        logging.debug('deployment_type: ' + str(deployment_type))

        # if aerospike stack, check that cluster name is no more than 25 chars (vendor limitation)
        if cluster_stack.lower() == 'aerospike':
            if len(cluster_name) > 25:
                status = 'Aerospike cluster name is limited to 25 chars only.'
                LogRequest('post_new_cluster', payload, 201, status, 'post')
                logging.error(status)
                update_task(task_id, 'Finished', 201, status)

        if deployment_type not in ('small', 'medium', 'large', 'None'):
            status = 'cluster_type is optional and should be only small, medium or large'
            LogRequest('post_new_cluster', payload, 201, status, 'post')
            logging.error(status)
            update_task(task_id, 'Finished', 201, status)

        gitcheckout_exit_code, temp_folder = gitcheckout('master', cluster_stack + '_git_path')
        project_dir = os.path.join(temp_folder, 'projects', project_name, cluster_name)
        check_kill_status(task_id)
        if gitcheckout_exit_code == 0:
            modify_exit_code, modify_error = modify_terraform_variables_cluster(temp_folder, payload.data,
                                                                                deployment_type, cluster_stack)
            if modify_exit_code == 0 and modify_error is None:
                check_kill_status(task_id)
                terraform_return_code, terraform_error = terraform_action(project_dir, 'apply', terraform_lock='no')
                if terraform_return_code == 0:
                    # Write the payload to the new project dir
                    write_payload_to_json(payload.data, project_dir)
                    check_kill_status(task_id)
                    commit_exit_code, commit_error = \
                        gitcommitpush(temp_folder, 'DI-Manager: NEW ' + cluster_stack + ' Cluster created at: '
                                      + project_name + '@' + cluster_name)
                    if commit_exit_code == 0:
                        status = 'Successfully deployed new ' + cluster_stack + ' Cluster ' + project_name +\
                                 '@' + cluster_name
                        status_code = 200
                    else:
                        status = commit_error
                        status_code = 202
                else:
                    check_kill_status(task_id)
                    terraform_return_code, terraform_error = terraform_action(project_dir, 'destroy', terraform_lock='no')
                    if terraform_return_code == 0:
                        logging.debug("The failed run was terraform destroyed successfully")
                    if terraform_error is None:
                        terraform_error = 'Unable to create/apply terraform code'
                    status = terraform_error
                    status_code = 202
            else:
                if modify_error is None:
                    modify_error = 'Unable to create/apply terraform code'
                status = modify_error
                status_code = 202
        else:
            status = 'Unable to checkout code from git'
            status_code = 202
        mail_dict = {"subject": "DI Manager: " + str(status),
                     "body": str(payload.data) + os.linesep + os.linesep +
                     "Status: " + str(status)}
        LogRequest('create_new_cluster', payload, status_code, status, 'post')
        sendemail(target_mail_address, mail_dict)
        if status_code == 200:
            LogDeployment(payload, 'online', cluster_stack)
            success_cleanup(temp_folder)
        update_task(str(task_id), 'Finished', status_code, str(status)[:65535])
    except Exception as error:
        logging.error(str(error))
        update_task(str(task_id), 'Finished', 500, str(status)[:65535])
        sys.exit(2)



def delete_cluster(task_id, payload, p_project_id, p_cluster_name, p_cluster_type):
    """
    a generic function to drop a cluster (any type of tech), it will basically Terraform destroys
    the cluster and clean it from Git
    :param task_id:
    :param payload:  the user provided request
    :param p_project_id:  target GCP project where the cluster exists
    :param p_cluster_name:  target Cluster to be destroyed
    :param p_cluster_type:  the cluster tech type, ie kafka, elastic, aerospike etc
    :return:
    """
    
    logging.info('This is my task_id: ' + str(task_id))
    p_project_name = get_project_name_from_id(p_project_id)
    status_code = None
    status = None

    # Validate if project exists
    return_code, return_msg = check_project_exists(p_project_id)
    if return_code == 0 and return_msg == 'not exists':
        status = 'GCP Project: ' + str(p_project_name) + ' does not exists'
        logging.error(status)
        update_task(task_id, 'Finished', 202, status)
        return None

    if p_cluster_type.lower() == 'postgresql':
        cfg_git_path = 'pg_git_path'
    elif p_cluster_type.lower() == 'kafka':
        cfg_git_path = 'kafka_git_path'
    elif p_cluster_type.lower() == 'elastic':
        cfg_git_path = 'elastic_git_path'
    elif p_cluster_type.lower() == 'aerospike':
        cfg_git_path = 'aerospike_git_path'
    else:
        status = 'env_type should be only kafka, postgresql, elastic or aerospike'
        LogRequest('DeleteInstance', payload, 202, status, 'delete')
        update_task(str(task_id), 'Finished', 202, str(status))
        return None
    check_kill_status(task_id)
    folder_removed_status = 0
    while folder_removed_status == 0:
        gitcheckout_exit_code, temp_folder = gitcheckout('master', cfg_git_path)
        if gitcheckout_exit_code == 0:
            project_dir = os.path.join(temp_folder, 'projects', p_project_name, p_cluster_name)
            # Check that the cluster directory exists, ie that the cluster exists
            if not os.path.isdir(project_dir):
                status = ('Cluster: ' + str(p_cluster_name) + ' does not exists or is not managed by DI manager')
                logging.error(status)
                update_task(task_id, 'Finished', 202, status)
                return None
            check_kill_status(task_id)
            logging.info('Now Executing Terraform destroy on our project. ' + str(temp_folder))
            terraform_return_code, terraform_error = terraform_action(project_dir, 'destroy', terraform_lock='no')
            if terraform_return_code == 0:
                logging.info('Deleting project directory: ' + str(project_dir))
                shutil.rmtree(project_dir)
                commit_exit_code, commit_error = \
                    gitcommitpush(temp_folder, 'DI-Manager: ' + p_cluster_type + ' cluster destroyed at: '
                                  + p_project_name + '@' + p_cluster_name)
                # Let`s verify the project was removed.
                check_kill_status(task_id)
                gitcheckout_verify_exit_code, temp_verify_folder = gitcheckout('master', cfg_git_path)
                project_verify_dir = os.path.join(temp_verify_folder, 'projects', p_project_name, p_cluster_name)
                if os.path.isdir(project_verify_dir):
                    logging.info('Project: ' + str(p_project_name) + ' Still exists - retrying!')
                    folder_removed_status = 0
                else:
                    if commit_exit_code == 0:
                        status = 'Successfully Deleted ' + p_cluster_type + ' cluster ' + p_cluster_name
                        status_code = 200
                        folder_removed_status = 1
                        # if cluster is elastic then remove the exists files from GCS
                        if p_cluster_type.lower() == 'elastic':
                            delete_elastic_exists_files(p_project_name, p_project_id, p_cluster_name)
                    else:
                        status = commit_error
                        status_code = 202
                        folder_removed_status = 1
            else:
                if terraform_error is None:
                    terraform_error = 'terraform delete failed'
                status = terraform_error
                status_code = 202
                folder_removed_status = 1
        else:
            status = 'Unable to check Git repo'
            status_code = 202
            folder_removed_status = 1
    logging.info('status: ' + str(status))
    if status_code == 200:
        delete_cluster_record(p_cluster_name, p_project_id, p_cluster_type)
    LogRequest('DeleteInstance', payload, status_code, status, 'delete')
    update_task(str(task_id), 'Finished', status_code, str(status))


def delete_elastic_exists_files(project_name, project_id, p_cluster_name):
    """
    any new elastic cluster has exists file in GCS, those files should exist as long as the cluster is up once
     destroyed they should be removed.
    :param project_name:
    :param project_id:
    :param p_cluster_name:
    :return:
    """

    try:
        logging.info('Lets remove the exists files form gcs bucket')
        cfg_results = getdefaultvalues()
        gcloud_path = cfg_results.get('gcloud_path')
        command = gcloud_path + " auth activate-service-account --key-file=/root/gcp_key.json; " + \
                  gcloud_path + " config set project " + project_id + "; " +  \
                  "/root/google-cloud-sdk/bin/gsutil rm -r gs://data-" + project_name + "/elastic_clusters_status/" + p_cluster_name + ""

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
        logging.error('error: ' + str(e))

@retry(stop=(stop_after_attempt(10)))
def LogDeployment(request, status, cluster_stack):
    """
    Based on info, create an Audit table log entry representing this api request.

    :view_name, the class/function name in views.py calling this LogRequest
    :request, the request api payload
    :exit_code, the http exit status code (ie 200, 400 etc)
    :response, the response payload.
    :http_verb, the http verb ie GET, PUT, POST, DELETE etc
    """
    try:
        logging.info('Logging new deployment data to the database.')
        request_data = request.data
        environment = GetKeyValueFromDict(request_data, 'env_type')
        cluster_name = GetKeyValueFromDict(request_data, 'cluster_name')
        username = request.user
        region = GetKeyValueFromDict(request_data, 'region')
        project_id = GetKeyValueFromDict(request_data, 'project_id')
        project_name = get_project_name_from_id(project_id)

        entry = deployment_overview(environment=environment, cluster_stack=cluster_stack, cluster_name=cluster_name,
                                    username=username, region=region, project_name=project_name,
                                    project_id=project_id, status=status)
        entry.save()

    except Exception as e:
        logging.error('error: ' + str(e))
