#from di_app_manager.app_code.validation import *
#from di_app_manager.app_code.git_procs import *
#from di_app_manager.app_code.cluster_deployment import LogDeployment


def create_gcp_project(task_id, payload, action):
    """
    The entire post code for the creation of a new GCP project, or aligning a pre-existing project
    :param action: create or align; create: will create a new GCP project, align: GCP project
    :param task_id:
    :param payload: the user provided JSON with all the info needed to create a new cluster
    :return: Response(<info dict>, status_code- ie 200 or 400 etc...)
    """
    try:
        
        logging.info('This is my task_id: ' + str(task_id))
        target_mail_address = GetKeyValueFromDict(payload.data, 'target_mail_address')
        env_type = GetKeyValueFromDict(payload.data, 'env_type')
        fqdn = GetKeyValueFromDict(payload.data, 'certificates_fqdn')
        project_name = GetKeyValueFromDict(payload.data, 'project_name')
        logging.debug('project_name: ' + str(project_name))
        logging.debug('env_type: ' + str(env_type))

        # Verify env_type is valid (dev dr or prod)
        project_id = None
        env_type = env_type.lower()
        if not env_type == 'dev' and not env_type == 'dr' and not env_type == 'prod' and not env_type == 'csdev':
            status = str('env types must be either: prod, dr, dev or csdev')
            logging.error(status)
            LogRequest('create_gcp_project', payload, 201, status, 'post')
            update_task(task_id, 'Finished', 201, status)
            return None

        if len(project_name) > 30:
            status = str('project_name should be up to 30 character')
            logging.error(status)
            LogRequest('create_gcp_project', payload, 201, status, 'post')
            update_task(task_id, 'Finished', 201, status)
            return None

        # Validate if project already exists
        return_code, return_msg = check_project_exists(project_name)
        if return_code == 0 and return_msg == 'exists' and action == 'deploy':
            status = str('GCP project: ' + project_name + ' already exists')
            logging.error(status)
            LogRequest('create_gcp_project', payload, 201, status, 'post')
            update_task(task_id, 'Finished', 201, status)
            return None
        if return_code == 0 and return_msg == 'not exists' and action == 'align':
            status = str('GCP project: ' + project_name + ' does not exists')
            logging.error(status)
            LogRequest('create_gcp_project', payload, 201, status, 'post')
            update_task(task_id, 'Finished', 201, status)
            return None

        gitcheckout_exit_code, temp_folder = gitcheckout('master', 'project_git_path')
        # prepare template and target dir paths
        template_path = os.path.join(temp_folder, 'projects', 'template/')
        logging.debug('template_path: ' + str(template_path))
        target_path = os.path.join(temp_folder, 'projects', project_name)
        logging.debug('target_path: ' + str(target_path))
        # Let`s check if the target dir already exists
        if os.path.isdir(target_path):
            status = str('GCP project: ' + project_name + ' is already managed by DS manager')
            logging.error(status)
            LogRequest('create_gcp_project', payload, 201, status, 'post')
            update_task(task_id, 'Finished', 201, status)
            return None

        # copy from template dir to target  new project/instance directory.
        logging.info('Let`s copy a new project code from template')
        shutil.copytree(template_path, target_path)
        logging.info('Copy files from ' + str(template_path) + ' To ' + str(target_path))
        vars_file_full_path = os.path.join(target_path, 'variables.tf')

        # Now prepare the project ID, if align then get project id from payload, else project id = project name
        if action == 'align':
            project_id = GetKeyValueFromDict(payload.data, 'project_id')
        elif action == 'deploy':
            project_id = project_name

        changes_dict = {'!project_name': str(project_name), '!env_type': str(env_type),
                        '!project_id': str(project_id), '!fqdn': str(fqdn)}

        def replace_all(text, dic):
            for i, j in dic.items():
                j = str(j)
                text = text.replace(i, j)
            return text

        # Read the new instance variable file and replace the values with the payload values
        with open(vars_file_full_path) as f:
            var_text = f.read()
            var_text = replace_all(var_text, changes_dict)
        with open(vars_file_full_path, "w") as f:
            f.write(var_text)
        logging.debug('finished executing modify_terraform_variables_cluster')

        check_kill_status(task_id)
        project_dir = os.path.join(temp_folder, 'projects', project_name)
        if gitcheckout_exit_code == 0:
            os.chdir(temp_folder)
            terragrunt_hcl = os.path.join(temp_folder, 'terragrunt.hcl')
            terragrunt_hcl_nohook = os.path.join(temp_folder, 'terragrunt.hcl.nohook')
            terragrunt_hcl_hook = os.path.join(temp_folder, 'terragrunt.hcl.hook')
            if action == 'deploy':
                os.rename(terragrunt_hcl_nohook, terragrunt_hcl)
                terraform_action(project_dir, 'apply -target=module.new-ds-project', terraform_lock='no')
                os.chdir(temp_folder)
                os.rename(terragrunt_hcl, terragrunt_hcl_nohook)
                logging.debug(os.listdir())
                logging.debug(os.getcwd())
            os.rename(terragrunt_hcl_hook, terragrunt_hcl)
            logging.debug(os.listdir())
            logging.debug(os.getcwd())
            check_kill_status(task_id)
            terraform_return_code, terraform_error = terraform_action(project_dir, 'apply -target=module.align-ds-project',
                                                                      terraform_lock='no')
            os.chdir(temp_folder)
            os.rename(terragrunt_hcl, terragrunt_hcl_hook)
            logging.debug(os.listdir())
            logging.debug(os.getcwd())
            if terraform_return_code == 0:
                commit_exit_code, commit_error = gitcommitpush(temp_folder, 'DI-Manager: '
                                                               + project_name + ' GCP project ' + str(action))
                if commit_exit_code == 0:
                    status = 'Successfully ' + action + ' new GCP project: ' + str(project_name)
                    status_code = 200
                    # Write the payload to the new project dir
                    write_payload_to_json(payload.data, project_dir)
                else:
                    status = 'Failed to ' + action + ' new GCP project: ' + str(project_name)
                    status_code = 202
            else:
                check_kill_status(task_id)
                if action == 'deploy':
                    terraform_return_code, terraform_error = terraform_action(project_dir, 'destroy', terraform_lock='no')
                if terraform_return_code == 0 and action == 'deploy':
                    logging.debug("The failed run was terraform destroyed successfully, task_id: " + str(task_id))
                if terraform_error is None:
                    terraform_error = 'Unable to create/apply terraform code'
                status = str(terraform_error)
                status_code = 202
        else:
            status = 'Unable to checkout code from git'
            status_code = 202
        mail_dict = {"subject": 'DI Manager: ' + str(status),
                     "body": str(payload.data) + os.linesep + os.linesep +
                     "Status: " + str(status)}
        LogRequest('create_gcp_project', payload, status_code, status, 'post')
        sendemail(target_mail_address, mail_dict)
        if status_code == 200:
            success_cleanup(temp_folder)
        update_task(task_id, 'Finished', status_code, status)
    except Exception as e:
        logging.error('error: ' + str(e))


def update_packer_image(task_id, payload):
    """
    The function will execute new packer build for target GCP project and
    target tech stack (ie kafka/aerospike or elastic)
    :param task_id:
    :param payload: the user provided JSON with all the info needed to create a new cluster
    :return:
    """
    try:
        
        logging.info('This is my task_id: ' + str(task_id))
        target_mail_address = GetKeyValueFromDict(payload.data, 'target_mail_address')
        packer_git_version = GetKeyValueFromDict(payload.data, 'packer_git_version')
        project_id = GetKeyValueFromDict(payload.data, 'project_id')
        fqdn = GetKeyValueFromDict(payload.data, 'certificates_fqdn')
        cluster_stack = (GetKeyValueFromDict(payload.data, 'cluster_stack')).lower()
        logging.debug('cluster_stack: ' + str(cluster_stack))
        cfg_results = getdefaultvalues()
        env = cfg_results.get('env')

        # Validate cluster_stack is a valid value
        if cluster_stack not in ('kafka', 'elastic', 'aerospike'):
            status = 'technology_stack must be kafka/ aerospike or elastic'
            LogRequest('update_packer_image', payload, 201, status, 'post')
            logging.error(status)
            update_task(task_id, 'Finished', 201, status)

        # Validate if project already exists
        return_code, return_msg = check_project_exists(project_id)
        if return_code == 0 and return_msg == 'not exists':
            status = 'GCP Project: ' + str(project_id) + ' does not exists'
            logging.error(status)
            update_task(str(task_id), 'Finished', 202, str(status), None)

        gitcheckout_exit_code, temp_folder = gitcheckout(packer_git_version, cluster_stack + '_packer_git_path')
        check_kill_status(task_id)
        if gitcheckout_exit_code == 0:

            template_file = os.path.join(temp_folder + '/template.json')
            template_file_bck = os.path.join(temp_folder + '/template.json.bck')

            # Copy the template.json file
            shutil.copyfile(template_file, template_file_bck)

            def replace_a_line(old_text, new_line, filein, fileout):
                with open(filein) as fin, open(fileout, 'w') as file_out:
                    for line in fin:
                        lineout = line
                        if (line.strip()).startswith(old_text):
                            lineout = f"{new_line}\n"
                        file_out.write(lineout)

            def execute(command_string):
                process = subprocess.Popen(command_string, shell=True, bufsize=1,
                                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='utf-8',
                                           errors='replace')
                while True:
                    realtime_output = process.stdout.readline()
                    if realtime_output == '' and process.poll() is not None:
                        packer_return_code = process.returncode
                        break
                    if realtime_output:
                        print(realtime_output.strip(), flush=False)
                        sys.stdout.flush()
                return packer_return_code

            text = '"project_id":'   # if any line contains this text, I want to modify the whole line.
            new_text = '    "project_id": "' + project_id + '",'

            replace_a_line(text, new_text, template_file_bck, template_file)

            # Copy the template.json file
            shutil.copyfile(template_file, template_file_bck)

            text = '"env":'   # if any line contains this text, I want to modify the whole line.
            new_text = '    "env": "' + env + '",'

            replace_a_line(text, new_text, template_file_bck, template_file)

            # Copy the template.json file
            shutil.copyfile(template_file, template_file_bck)

            text = '"certificates_fqdn":'   # if any line contains this text, I want to modify the whole line.
            new_text = '    "certificates_fqdn": "' + fqdn + '",'

            replace_a_line(text, new_text, template_file_bck, template_file)

            # Execute packer build
            my_env = os.environ.copy()
            my_env["GOOGLE_APPLICATION_CREDENTIALS"] = "/root/gcp_key.json"
            my_env["GOOGLE_CREDENTIALS"] = "/root/gcp_key.json"
            logging.debug(my_env)
            cfg_results = getdefaultvalues()
            gcloud_path = cfg_results.get('gcloud_path')
            command = "export GOOGLE_APPLICATION_CREDENTIALS=/root/gcp_key.json; export GOOGLE_CREDENTIALS=/root/gcp_key.json;" + \
              gcloud_path + " config set project " + project_id + "; packer build -force template.json"
            os.chdir(temp_folder)
            logging.debug('Changing dir to: ' + str(temp_folder))
            logging.debug('Now executing: ' + str(command))
            # process = subprocess.run(command, capture_output=True, text=True, shell=True)
            exit_code = execute(command)
            logging.debug('Executed with status code:' + str(exit_code))
            if exit_code == 0:
                status_code = 200
                status = 'Packer build successfully executed for ' + cluster_stack + ' at ' + project_id
            else:
                status_code = 500
                status = 'Packer build failed at ' + cluster_stack + ' at ' + project_id

        else:
            status = 'Unable to checkout code from git'
            status_code = 202
        mail_dict = {"subject": "DI Manager: " + str(status),
                     "body": str(payload.data) + os.linesep + os.linesep + "Status: " + str(status)}
        LogRequest('create_new_cluster', payload, status_code, status, 'post')
        sendemail(target_mail_address, mail_dict)
        if status_code == 200:
            #LogDeployment(payload, 'online', cluster_stack)
            success_cleanup(temp_folder)
        update_task(str(task_id), 'Finished', status_code, str(status)[:65535])
    except Exception as e:
        logging.error('error: ' + str(e))

