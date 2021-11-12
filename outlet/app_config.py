import threading
from typing import Any

import config
import os
import json
import logging

import logging_config
from constants import DEFAULT_CONFIG_PATH, PROJECT_DIR, PROJECT_DIR_TOKEN
from util.file_util import get_resource_path

logger = logging.getLogger(__name__)


class ConfigRequest:
    def __init__(self, cfg_path: str, default_val: Any = None, is_required: bool = True):
        self.cfg_path: str = cfg_path
        self.default_val: Any = default_val
        self.is_required: bool = is_required


class AppConfig:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS AppConfig
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, config_file_path: str = None, executing_script_name: str = None):
        self._project_dir = get_resource_path(PROJECT_DIR)
        self._lock = threading.Lock()

        if not config_file_path:
            config_file_path = get_resource_path(DEFAULT_CONFIG_PATH)

        try:
            print(f'Reading config file: "{config_file_path}"')
            self._cfg = config.Config(config_file_path)
            # Cache JSON in memory rather than risk loading a corrupted JSON file later while we're about
            # to write something
            with self._lock:
                with open(self._get_ui_state_filename()) as f:
                    self._ui_state_json = json.load(f)

            self.read_only = self.get_config('read_only_config', False, is_required=False)
        except Exception as err:
            raise RuntimeError(f'Could not read config file ({config_file_path})') from err

        logging_config.configure_logging(self, executing_script_name)
        if self.read_only:
            logger.info('Config is set to read-only')

    def get_config_from_request(self, request: ConfigRequest):
        return self.get(cfg_path=request.cfg_path, default_val=request.default_val, required=request.is_required)

    def get_config(self, cfg_path: str, default_val=None, is_required: bool = True):
        return self.get(cfg_path=cfg_path, default_val=default_val, required=is_required)

    # TODO: Deprecate this! use get_config (better name for searches)
    def get(self, cfg_path: str, default_val=None, required: bool = True):
        try:
            val = self._cfg[cfg_path]
            if val is None and default_val is None and required:
                raise RuntimeError(f'Config entry not found but is required: "{cfg_path}"')

            if val is not None and type(val) == str:
                val = val.replace(PROJECT_DIR_TOKEN, self._project_dir)
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
                assert segment == 'ui_state', 'Only ui_state may be written to!'
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
