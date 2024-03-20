from di_app_manager.app_code.utils import *


@retry(stop=(stop_after_attempt(10)))
def check_project_exists(project_id):
    """
    Check if Project exists

    :param project_id: the target project id
    :return: 0 success run , return msg: exists or not exists
    """
    try:
        logging.info('Lets check if: ' + str(project_id) + ' exists')
        cfg_results = getdefaultvalues()
        gcloud_path = cfg_results.get('gcloud_path')
        # Let`s activate the service account
        activate_sa = gcloud_path + " auth activate-service-account --key-file=/root/gcp_key.json"
        process = subprocess.run(activate_sa + "; " + gcloud_path + " projects list |grep " + project_id + " | wc -l",
                                 capture_output=True, text=True, shell=True)
        set_error = process.stderr
        set_status = process.returncode
        set_out = process.stdout
        logging.debug('Project list output: ' + str(set_out))
        if int(set_out) != 0:
            logging.debug('Project: ' + project_id + ' already exists')
            return 0, 'exists'
        else:
            logging.debug('Project: ' + project_id + ' does not exists')
            return 0, 'not exists'
    except Exception as e:
        logging.error(str(e))
        return 1, str(e)


def verify_pg_version(input_version):
    """
    get a supported PG versions list from CFG file, then check if provided version is supported.

    :param input_version: provided PG version
    :return: 0 - version is supported, 1 - version is not supported
    """
    logging.info('Let`s verify the provided pg version: ' + str(input_version))
    cfg_results = getdefaultvalues()
    supported_versions = cfg_results.get('supported_versions')
    supported_versions = (list(supported_versions.split(",")))
    logging.debug('Current support list: ' + str(supported_versions))

    if input_version not in supported_versions:
        logging.error('Unsupported PG version')
        return 1
    else:
        logging.debug('PG version supported, Let`s move on')
        return 0


def verify_region(input_region):
    """
    get a supported Regions list from CFG file, then check if provided region is supported.

    :param input_region: Provided region
    :return: 0 - version is supported, 1 - version is not supported
    """
    logging.info('Let`s verify the provided region: ' + str(input_region))
    cfg_results = getdefaultvalues()
    supported_regions = cfg_results.get('supported_regions')
    supported_regions = (list(supported_regions.split(",")))
    logging.debug('Current regions supported list: ' + str(supported_regions))

    if input_region not in supported_regions:
        logging.error('Unsupported Region ' + input_region)
        return 1
    else:
        logging.debug('Region supported, Let`s move on')
        return 0


def validity_parameters(results_dict, cluster_stack):
    """
    based on results_dict dict, parse the dict and execute checks on parameter based on cluster_stack

    :param results_dict:
    :param cluster_stack:
    :return:
    """
    logging.info('Let`s validity_parameters')
    try:
        # Pull default values from cfg file
        cfg_results = getdefaultvalues()
        disk_size_limit_gb = cfg_results.get('disk_size_limit_gb')
        # Verify disk_size is valid positive number
        data_disk_size = results_dict[cluster_stack + '_data_disk_size']
        logging.debug('data_disk_size: ' + str(data_disk_size))
        if type(data_disk_size) == int:
            if data_disk_size < 1 or data_disk_size > int(disk_size_limit_gb):
                logging.debug('disk_size must be a positive number between 1 to 10000 (GB)')
                return 1, 'disk_size must be a positive number between 1 to 10000 (GB)'
        elif data_disk_size.isnumeric():
            disk_size = int(data_disk_size)
            if disk_size < 1 or disk_size > int(disk_size_limit_gb):
                logging.debug('disk_size must be a positive number between 1 to 10000 (GB)')
                return 1, 'disk_size must be a positive number between 1 to 10000 (GB)'
        else:
            logging.debug('disk_size must be a positive number between 1 to 10000 (GB)')
            return 1, 'disk_size must be a positive number between 1 to 10000 (GB)'
        logging.debug('validity_parameters proc successfully completed, return_code = 0')
        return 0, 'all is good'
    except Exception as e:
        logging.error('error: ' + str(e))


@retry(stop=(stop_after_attempt(10)))
def get_project_name_from_id(project_id):
    """
    using gcloud get the project name from the project id provided

    :param project_id:
    :return:
    """

    try:

        logging.info('convert project id : ' + str(project_id) + ' to project name')
        cfg_results = getdefaultvalues()
        gcloud_path = cfg_results.get('gcloud_path')
        # Let`s activate the service account
        command = gcloud_path + " auth activate-service-account --key-file=/root/gcp_key.json; " + \
                  gcloud_path + " config set project " + project_id + '; ' + \
                  gcloud_path + " projects list |grep " + project_id + " | awk '{print $2}'"
        logging.debug('Executing command: ' + str(command))
        process = subprocess.run(command, capture_output=True, text=True, shell=True)
        set_error = process.stderr
        set_status = process.returncode
        project_name = process.stdout
        project_name = str(project_name).strip()
        logging.info('project id: ' + project_id + ' is project_name: ' + project_name)

        return project_name
    except Exception as e:
        logging.error('error: ' + str(e))