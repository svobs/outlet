import threading
from typing import Any

import config
import os
import json
import logging

import logging_config
from constants import DEFAULT_CONFIG_PATH, EXE_NAME_TOKEN, PROJECT_DIR, PROJECT_DIR_TOKEN, UI_STATE_CFG_SEGMENT
from util.file_util import get_resource_path

logger = logging.getLogger(__name__)


class AppConfig:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS AppConfig
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, config_file_path: str = None, executing_script_name: str = None):
        self._project_dir = get_resource_path(PROJECT_DIR)
        self._lock = threading.Lock()
        self._executing_script_name = executing_script_name

        if not config_file_path:
            config_file_path = get_resource_path(DEFAULT_CONFIG_PATH)

        try:
            print(f'Reading config file: "{config_file_path}"')
            self._cfg = config.Config(config_file_path)

            self.read_only = self.get_config('read_only_config', False, required=False)
        except Exception as err:
            raise RuntimeError(f'Could not read config file "{config_file_path}"') from err

        ui_state_filename = self._get_ui_state_filename()
        try:
            # Cache JSON in memory rather than risk loading a corrupted JSON file later while we're about
            # to write something
            with self._lock:
                print(f'Reading JSON config file: "{ui_state_filename}"')
                with open(ui_state_filename) as f:
                    self._ui_state_json = json.load(f)

        except Exception as err:
            raise RuntimeError(f'Could not read JSON config file "{ui_state_filename}"') from err

        logging_config.configure_logging(self)
        if self.read_only:
            logger.info('Config is set to read-only')
            
    def get_project_dir(self) -> str:
        return self._project_dir

    def get_config(self, cfg_path: str, default_val=None, required: bool = True):
        try:
            val = self._cfg[cfg_path]
            if val is None and default_val is None and required:
                raise RuntimeError(f'Config entry not found but is required: "{cfg_path}"')

            if val is not None and type(val) == str:
                val = val.replace(PROJECT_DIR_TOKEN, self._project_dir)
                val = val.replace(EXE_NAME_TOKEN, self._executing_script_name)
            logger.debug(f'Read config entry "{cfg_path}" = "{val}"')
            return val
        except (KeyError, config.KeyNotFoundError):
            logger.debug(f'Path not found: {cfg_path}')

        # throw this outside the except block above (putting it in the block seems to print out 2 extra exceptions):
        if required:
            raise RuntimeError(f'Path not found but is required: "{cfg_path}"')
        return default_val

    def _get_ui_state_filename(self):
        filename = self.get_config('ui_state_filename')
        return os.path.join(self._cfg.rootdir, filename)

    def write(self, json_path: str, value: Any, insert_new_ok=True):
        if self.read_only:
            logger.debug(f'Ignoring change to config "{json_path}"; we are read-only')
            return

        assert json_path is not None
        assert value is not None, f'For path "{json_path}"'

        # Update JSON in memory:
        path_segments = json_path.split('.')
        sub_dict = self._ui_state_json
        last = len(path_segments) - 1
        for num, segment in enumerate(path_segments):
            if num == 0:
                if segment != UI_STATE_CFG_SEGMENT:
                    raise RuntimeError(f'Cannot write to path "{json_path}": Only paths starting with "{UI_STATE_CFG_SEGMENT}" may be written to!')
                # otherwise just eat the first token: it is a shortcut to the JSON file
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
        with self._lock:
            with open(tmp_filename, 'w') as f:
                json.dump(self._ui_state_json, f, indent=4, sort_keys=True)
            os.rename(tmp_filename, json_file)

        logger.debug(f'Wrote {json_path} := "{value}" in file: {json_file}')
