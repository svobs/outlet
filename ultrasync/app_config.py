import config
from file_util import get_resource_path

DEFAULT_CONFIG_PATH = get_resource_path('default.cfg')
PROJECT_DIR = get_resource_path('.')


class AppConfig:
    def __init__(self, config_file_path=DEFAULT_CONFIG_PATH):
        try:
            self.cfg = config.Config(config_file_path)
        except Exception as err:
            raise RuntimeError(f'Could not read config file ({config_file_path})') from err

    def get(self, cfg_path):
        val = self.cfg[cfg_path]
        if val is not None and type(val) == str:
            val = val.replace('$PROJECT_DIR', PROJECT_DIR)
        return val
