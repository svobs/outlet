from typing import Any

import config
import os
import json
import logging

import logging_config
from constants import DEFAULT_CONFIG_PATH, PROJECT_DIR
from util.file_util import get_resource_path

logger = logging.getLogger(__name__)
PROJECT_DIR_TOKEN = '$PROJECT_DIR'


class AppConfig:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS AppConfig
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, config_file_path: str = None, executing_script_name: str = None):
        self._project_dir = get_resource_path(PROJECT_DIR)

        if not config_file_path:
            config_file_path = get_resource_path(DEFAULT_CONFIG_PATH)

        try:
            print(f'Reading config file: "{config_file_path}"')
            self.cfg = config.Config(config_file_path)
            # Cache JSON in memory rather than risk loading a corrupted JSON file later while we're about
            # to write something
            with open(self._get_ui_state_filename()) as f:
                self._ui_state_json = json.load(f)

            self.read_only = self.get('read_only_config', False)
        except Exception as err:
            raise RuntimeError(f'Could not read config file ({config_file_path})') from err

        logging_config.configure_logging(self, executing_script_name)
        if self.read_only:
            logger.info('Config is set to read-only')

    def get(self, cfg_path, default_val=None):
        try:
            val = self.cfg[cfg_path]
            if val is not None and type(val) == str:
                val = val.replace(PROJECT_DIR_TOKEN, self._project_dir)
            logger.debug(f'Read "{cfg_path}" = "{val}"')
            return val
        except config.KeyNotFoundError:
            logger.debug(f'Path not found: {cfg_path}')
            return default_val

    def _get_ui_state_filename(self):
        filename = self.get('ui_state_filename')
        return os.path.join(self.cfg.rootdir, filename)

    def write(self, json_path: str, value: Any, insert_new_ok=True):
        if self.read_only:
            logger.debug(f'No change to config "{json_path}"; we are read-only')
            return

        assert json_path is not None
        assert value is not None, f'For path "{json_path}"'

        # Update JSON in memory:
        path_segments = json_path.split('.')
        sub_dict = self._ui_state_json
        last = len(path_segments) - 1
        for num, segment in enumerate(path_segments):
            if num == 0:
                assert segment == 'ui_state'
            else:
                val = sub_dict.get(segment, None)
                if num == last:
                    if val == value:
                        logger.debug(f'No change to config {segment}')
                        return
                    sub_dict[segment] = value
                    logger.debug(f'Wrote value "{value}" to "{json_path}"')
                elif val is None:
                    if not insert_new_ok:
                        raise RuntimeError(f'Path segment "{segment}" not found in path "{json_path}"')
                    else:
                        sub_dict[segment] = {}
                        sub_dict = sub_dict[segment]
                else:
                    # go to next segment
                    sub_dict = val

        # Dump JSON to file atomically:
        json_file = self._get_ui_state_filename()
        tmp_filename = json_file + '.part'
        with open(tmp_filename, 'w') as f:
            json.dump(self._ui_state_json, f, indent=4, sort_keys=True)
        os.rename(tmp_filename, json_file)

        logger.info(f'Wrote {json_path} := "{value}" in file: {json_file}')
