from di_app_manager.app_code.cluster_deployment import LogDeployment
from di_app_manager.app_code.git_procs import *
from di_app_manager.app_code.validation import *
from django.http import JsonResponse


def state_check_api(request, cluster_stack):
    try:
        plan_output = None
        task_id = enable_logging()
        logging.info('task_id: ' + str(task_id))
        target_mail_address = GetKeyValueFromDict(request.data, 'target_mail_address')
        project_id = GetKeyValueFromDict(request.data, 'project_id')
        project_name = get_project_name_from_id(project_id)
        cluster_name = GetKeyValueFromDict(request.data, 'cluster_name')

        # Validate if project exists
        return_code, return_msg = check_project_exists(project_id)
        if return_code == 0 and return_msg == 'not exists':
            status = 'GCP Project: ' + str(project_id) + ' does not exists'
            logging.error(status)
            return JsonResponse({'status': status}, status=203)

        gitcheckout_exit_code, temp_folder = gitcheckout('master', cluster_stack + '_git_path')
        project_dir = os.path.join(temp_folder, 'projects', project_name, cluster_name)

        # Check that the cluster directory exists, ie that the cluster exists
        if not os.path.isdir(project_dir):
            status = ('Cluster: ' + str(cluster_name) + ' does not exists or is not managed by DI manager')
            logging.error(status)
            return JsonResponse({'status': status}, status=203)

        if gitcheckout_exit_code == 0:
            terraform_action(project_dir, 'init')
            plan_exit_code, plan_output = terraform_action(project_dir, 'plan')
            logging.debug('plan_exit_code: ' + str(plan_exit_code))
            if plan_exit_code == 0:
                status = ('Cluster: ' + str(cluster_name) + ' is aligned with Terraform state - no TF pending changes.')
                logging.info(status)
                status_code = 200
            else:
                status = ('Cluster: ' + str(cluster_name) + ' is not aligned with Terraform state -'
                                                            ' there are TF pending changes.')
                logging.info(str(plan_output))
                logging.info(status)
                status_code = 201

        else:
            status = 'Unable to Checkout git project'
            status_code = 500

        mail_dict = {"subject": "DI Manager: " + str(status),
                     "body": str(request.data) + os.linesep + os.linesep +
                     "Status: " + str(status)}
        LogRequest('state_check_api', request, status_code, status, 'post')
        LogDeployment(request, 'online', cluster_stack)
        sendemail(target_mail_address, mail_dict, str(plan_output))
        logging.info('status: ' + status)
        if status_code in (200, 201):
            success_cleanup(temp_folder)
        return JsonResponse({'status': status}, status=status_code)
    except Exception as e:
        logging.error('error: ' + str(e))
        return JsonResponse({'error': str(e)}, status=500)
