from di_app_manager.app_code.cluster_deployment import *


def terraform_apply(task_id, payload, cluster_stack):
    """
    Post code for terraform apply

    :param task_id: for multi threading
    :param payload: the user provided JSON with all the info needed to run terraform apply to create the cluster
    :param cluster_stack: elastic or kafka
    :return: statue code and return msg (error or success)
    """

    try:
        
        logging.info('This is my task_id: ' + str(task_id))
        target_mail_address = GetKeyValueFromDict(payload.data, 'target_mail_address')
        project_id = GetKeyValueFromDict(payload.data, 'project_id')
        project_name = get_project_name_from_id(project_id)
        cluster_name = GetKeyValueFromDict(payload.data, 'cluster_name')
        git_version = GetKeyValueFromDict(payload.data, cluster_stack + '_git_version')
        status = None
        status_code = None

        # Validate if project exists
        return_code, return_msg = check_project_exists(project_id)
        if return_code == 0 and return_msg == 'not exists':
            status = 'GCP Project: ' + str(project_id) + ' does not exists'
            logging.error(status)
            update_task(task_id, 'Finished', 202, status)
            return None

        gitcheckout_exit_code, temp_folder = gitcheckout('master', cluster_stack +'_git_path')
        project_dir = os.path.join(temp_folder, 'projects', project_name, cluster_name)

        # Check that the cluster directory exists, ie that the cluster exists
        if not os.path.isdir(project_dir):
            status = ('Cluster: ' + str(cluster_name) + ' does not exists or is not managed by DI manager')
            logging.error(status)
            update_task(task_id, 'Finished', 202, status)
            return None

        # Check that git version exists in case current version was not selected
        if git_version != 'current':
            os.chdir(project_dir)
            os.system('git fetch -t')
            git_version_exists = os.system('git describe --tags ' + git_version)
            if git_version_exists != 0:
                status = ('Git version : ' + str(git_version) + ' does not exists or it is not managed by DI manager')
                logging.error(status)
                update_task(task_id, 'Finished', 202, status)
                return None

        check_kill_status(task_id)
        if gitcheckout_exit_code == 0:
            if git_version != 'current':
                main_file_full_path = os.path.join(project_dir, 'main.tf')
                logging.info('main file location: ' + str(main_file_full_path))
                change_return_code, change_error = change_git_version(main_file_full_path, git_version)
                if change_return_code == 0:
                    check_kill_status(task_id)
                    logging.info('Git Version applied: ' + str(git_version))
                    terraform_return_code, terraform_error = terraform_action(project_dir, 'apply', terraform_lock='no')
                    if terraform_return_code == 0:
                        commit_exit_code, commit_error = \
                        gitcommitpush(temp_folder, 'DI-Manager: Applied ' + cluster_stack + ' cluster at: '
                                  + project_name + '@' + cluster_name + ' based on git version: ' + git_version)
                        if commit_exit_code == 0:
                            status = 'Successfully applied ' + cluster_stack + ' Cluster ' + project_name + '@' + cluster_name
                            status_code = 200
                        else:
                            status = commit_error
                            logging.warning('Git checkout failed, check and fix manually')
                            status_code = 202
                    else:
                        if terraform_error is None:
                            terraform_error = 'Unable to apply terraform code'
                        status = terraform_error
                        status_code = 202
                else:
                    status = 'Failed to modify project main.tf with the new git version' + change_error
                    status_code = 202
            else:
                check_kill_status(task_id)
                terraform_return_code, terraform_error = terraform_action(project_dir, 'apply', terraform_lock='no')
                if terraform_return_code == 0:
                    status = 'Successfully applied ' + cluster_stack + ' Cluster ' + project_name + '@' + cluster_name
                    status_code = 200
                else:
                    if terraform_error is None:
                        terraform_error = 'Unable to apply terraform code'
                    status = terraform_error
                    status_code = 202
        else:
            status = 'Unable to checkout code from git'
            status_code = 202

        mail_dict = {"subject": "DI Manager: " + str(status),
                     "body": str(payload.data) + os.linesep + os.linesep +
                             "Status: " + str(status)}
        LogRequest('terraform_apply', payload, status_code, status, 'post')
        LogDeployment(payload, 'online', cluster_stack)
        sendemail(target_mail_address, mail_dict)
        if status_code == 200:
            success_cleanup(temp_folder)
        update_task(str(task_id), 'Finished', status_code, str(status))

    except Exception as terraform_apply_error:
        logging.error('error: ' + str(terraform_apply_error))
        update_task(task_id, 'Finished', 500, 'Internal Error: ' + str(terraform_apply_error))


def change_git_version(main_file_full_path, git_version):
    """
    Replacing current git_version with git_version provided by the user

    :param main_file_full_path: The main.tf to read and replace git repository
    :param git_version: the user provided new git version to be set within project main.tf: following is the line to replace:
        source = "git@github.com:cybereason-labs/tf-di-kafka//module?ref=<git branch to replace>"
    :return: statue code and return msg (error or success)
    """

    try:

        # Reading main.tf file to find current git version
        fin = open(main_file_full_path, "r")
        text = "ref="
        lines = fin.readlines()

        # Read line by line and break on the first occurrence of the git reference
        logging.info('Searching for the current git version within ' + main_file_full_path)
        for line in lines:
            if text in line:
                break
        fin.close()

        line = line.split("=")
        line = str((line[-1]))
        current_version = line.replace('"', '').rstrip()
        logging.info('Found current version: ' + current_version)

        # Replacing the current git version with the one given by the user
        fin = open(main_file_full_path, "rt")
        data = fin.read()
        logging.info('Going to replace current git version: ' + current_version + ' With: ' + git_version)
        data = data.replace('ref=' + current_version, 'ref=' + git_version)
        fin.close()

        fin = open(main_file_full_path, "wt")
        fin.write(data)
        fin.close()
        return 0, None
    except Exception as change_error:
        logging.error('error: ' + str(change_error))
        return 1, str(change_error)


