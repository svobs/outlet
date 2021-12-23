import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)
EXE_NAME_TOKEN = '$EXE_NAME'


def configure_logging(app_config, executing_script_name: str):
    # Argument 'executing_script_name' is used to name the log file

    # create logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # DEBUG LOG FILE
    debug_log_enabled = app_config.get_config('logging.debug_log.enable')
    if debug_log_enabled:
        debug_log_path: str = app_config.get_config('logging.debug_log.full_path')
        debug_log_path = debug_log_path.replace(EXE_NAME_TOKEN, executing_script_name)
        debug_log_mode = app_config.get_config('logging.debug_log.mode')
        debug_log_fmt = app_config.get_config('logging.debug_log.format')
        debug_log_datetime_fmt = app_config.get_config('logging.debug_log.datetime_format')

        log_dir = Path(debug_log_path).parent
        try:
            os.makedirs(name=log_dir, exist_ok=True)
        except Exception as err:
            logger.error(f'Exception while making log dir: {log_dir}')
            raise
        debug_file_handler = logging.FileHandler(filename=debug_log_path, mode=debug_log_mode)
        debug_file_handler.setLevel(logging.DEBUG)

        debug_file_formatter = logging.Formatter(fmt=debug_log_fmt, datefmt=debug_log_datetime_fmt)
        debug_file_handler.setFormatter(debug_file_formatter)

        root_logger.addHandler(debug_file_handler)

    # CONSOLE
    console_enabled = app_config.get_config('logging.console.enable')
    if console_enabled:
        console_fmt = app_config.get_config('logging.debug_log.format')
        console_datetime_fmt = app_config.get_config('logging.debug_log.datetime_format')

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)

        console_formatter = logging.Formatter(fmt=console_fmt, datefmt=console_datetime_fmt)
        console_handler.setFormatter(console_formatter)

        # add console output to all loggers
        root_logger.addHandler(console_handler)

    info_loggers = app_config.get_config('logging.loglevel_info')
    for logger_name in info_loggers:
        logging.getLogger(logger_name).setLevel(logging.INFO)

    # --- Google API ---
    warning_loggers = app_config.get_config('logging.loglevel_warning')
    for logger_name in warning_loggers:
        logging.getLogger(logger_name).setLevel(logging.WARNING)
