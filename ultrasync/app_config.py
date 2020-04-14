import config
import os
import json
import logging
from file_util import get_resource_path

PROJECT_DIR = get_resource_path('.')
DEFAULT_CONFIG_PATH = os.path.join(PROJECT_DIR, 'default.cfg')

logger = logging.getLogger(__name__)


class AppConfig:
    def __init__(self, config_file_path=DEFAULT_CONFIG_PATH):
        try:
            self.cfg = config.Config(config_file_path)
            with open(self._get_transient_filename()) as f:
                self.transient_json = json.load(f)
        except Exception as err:
            raise RuntimeError(f'Could not read config file ({config_file_path})') from err

    def get(self, cfg_path):
        val = self.cfg[cfg_path]
        if val is not None and type(val) == str:
           val = val.replace('$PROJECT_DIR', PROJECT_DIR)
        return val

    def _get_transient_filename(self):
        filename = self.get('transient_filename')
        return os.path.join(self.cfg.rootdir, filename)

    def write(self, transient_path, value):
        assert transient_path is not None
        assert value is not None

        # Update JSON in memory:
        path_segments = transient_path.split('.')
        sub_dict = self.transient_json
        last = len(path_segments) - 1
        for num, segment in enumerate(path_segments):
            if num == 0:
                assert segment == 'transient'
            elif num == last:
                sub_dict[segment] = value
            else:
                sub_dict = sub_dict[segment]

        # Dump JSON to file atomically:
        json_file = self._get_transient_filename()
        tmp_filename = json_file + '.part'
        with open(tmp_filename, 'w') as f:
            json.dump(self.transient_json, f, indent=4, sort_keys=True)
        os.rename(tmp_filename, json_file)

        logger.info(f'Wrote {transient_path} := "{value}" in file: {json_file}')