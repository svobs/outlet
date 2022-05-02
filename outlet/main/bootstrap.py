import os
import shutil
import sys

from constants import CONFIG_PY_DIR, INIT_FILE, LOGGING_CONSTANTS_FILE, PROJECT_DIR, TEMPLATE
from util import file_util
from util.file_util import get_resource_path


def configure():
    config_py_dir = get_resource_path(CONFIG_PY_DIR)
    file_util.touch(os.path.join(config_py_dir, INIT_FILE))
    sys.path.insert(0, config_py_dir)

    logging_constants_py = os.path.join(config_py_dir, LOGGING_CONSTANTS_FILE)
    if not os.path.exists(logging_constants_py):
        template_dir = os.path.join(get_resource_path(PROJECT_DIR), TEMPLATE)
        template_py = os.path.join(template_dir, f'_{LOGGING_CONSTANTS_FILE}')

        os.makedirs(name=config_py_dir, exist_ok=True)
        shutil.copyfile(template_py, dst=logging_constants_py, follow_symlinks=False)
