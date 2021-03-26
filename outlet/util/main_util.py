import os
import sys

from app_config import AppConfig


def do_main_boilerplate(executing_script_path: str = None) -> AppConfig:
    if executing_script_path:
        executing_script_name = os.path.basename(executing_script_path)
        if executing_script_name.endswith('.py'):
            executing_script_name = executing_script_name.replace('.py', '')
    else:
        executing_script_name = None

    if sys.version_info[0] < 3:
        raise Exception("Python 3 or a more recent version is required.")

    if len(sys.argv) >= 2:
        app_config = AppConfig(config_file_path=sys.argv[1], executing_script_name=executing_script_name)
    else:
        app_config = AppConfig(executing_script_name=executing_script_name)

    return app_config
