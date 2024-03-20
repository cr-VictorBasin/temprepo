from di_app_manager.app_code.utils import *
import git
import tempfile
from git import Repo


@retry(stop=(stop_after_attempt(10)))
def gitcommitpush(target_dir, desc):
    logging.info('About to Git repo commit and push')
    """
    git commit & push the file/directory to the relevant repo

    :param target_dir string, Target dir to with git repo to commit & push
    :param desc dict, the comment we want to added to the git commit
    """
    try:
        os.chdir(target_dir)
        repo = Repo(target_dir, search_parent_directories=True)
        repo.git.add(all=True)
        repo.index.commit(desc)
        origin = repo.remote('origin')
        origin.pull()
        origin.push()
        logging.info('Git repo committed and pushed')
        return 0, None
    except Exception as e:
        logging.error('Unable to commit & push)')
        logging.error('error: ' + str(e))
        return 1, str(e)


@retry(stop=(stop_after_attempt(10)))
def gitcheckout(project_name, cfg_git_path):
    """
    create a temp directory and checkout the entire git Terraform repo

    :param cfg_git_path: The Git path to the relevant code
    :param project_name: The project/git branch name (derived from argparse)
    :return temp_dir, string, location of the temp dir with the git repo:
    """
    try:
        logging.info('Let`s get the code from git')
        cfg_results = getdefaultvalues()
        git_path = cfg_results.get(cfg_git_path)

        temp_dir = tempfile.mkdtemp()
        git.Repo.clone_from(git_path, temp_dir, branch=project_name, depth=1)
        return 0, temp_dir
    except Exception as e:
        logging.error('error: ' + str(e))
        sys.exit(1)
