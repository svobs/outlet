import logging
import os
from datetime import datetime, timezone
from logging import handlers

from util.ensure import ensure_bool

logger = logging.getLogger(__name__)


class TimeOfLaunchRotatingFileHandler(handlers.RotatingFileHandler):

    def __init__(self, log_dir: str, filename_base: str, mode='a', maxbytes=0, backupcount=0, encoding=None, delay=0):
        """
        @summary:
        Set self.base_filename to include datetime string of now.
        The handler create logFile named self.base_filename
        """
        self.log_dir = log_dir
        self.filename_base = filename_base

        self.logfile_path = self._generate_logfile_path()

        handlers.RotatingFileHandler.__init__(self, self.logfile_path, mode, maxbytes, backupcount, encoding, delay)

    def _generate_logfile_path(self):
        """
        @summary: Return logFile name string formatted to "today.log.alias"
        """
        timestamp_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d_%H%M%S")
        filename = f'{self.filename_base}{timestamp_str}.log'
        return os.path.join(self.log_dir, filename)

    def shouldRollover(self, record):
        """Never rollover. It seems a fair assumption that we will not launch the app twice in the same second, but even if we do, the worst
        that will happen is that either the first is overwritten (if filemode 'w') which we almost certainly won't care about, or the second
        is appended to the first (if filemode 'a'), in which we have a small amount of excess lines which we can safely ignore."""
        return 0


def configure_logging(app_config):
    # Argument 'executing_script_name' is used to name the log file

    # create logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # --- DEBUG LOG FILE ---
    debug_log_enabled = ensure_bool(app_config.get_config('logging.debug_log.enable'))
    if debug_log_enabled:
        log_dir = app_config.get_config('logging.debug_log.log_dir')
        filename_base: str = app_config.get_config('logging.debug_log.filename_base')
        debug_log_mode = app_config.get_config('logging.debug_log.filemode')
        debug_log_fmt = app_config.get_config('logging.debug_log.format')
        debug_log_datetime_fmt = app_config.get_config('logging.debug_log.datetime_format')

        try:
            os.makedirs(name=log_dir, exist_ok=True)
        except Exception:
            logger.error(f'Exception while making log dir: {log_dir}')
            raise

        debug_file_handler = TimeOfLaunchRotatingFileHandler(log_dir=log_dir, filename_base=filename_base, mode=debug_log_mode)
        debug_file_level = logging.getLevelName(app_config.get_config('logging.debug_log.level'))
        debug_file_handler.setLevel(debug_file_level)

        debug_file_formatter = logging.Formatter(fmt=debug_log_fmt, datefmt=debug_log_datetime_fmt)
        debug_file_handler.setFormatter(debug_file_formatter)

        root_logger.addHandler(debug_file_handler)

    # --- CONSOLE ---
    console_enabled = app_config.get_config('logging.console.enable')
    if console_enabled:
        console_fmt = app_config.get_config('logging.console.format')
        console_datetime_fmt = app_config.get_config('logging.console.datetime_format')

        console_handler = logging.StreamHandler()
        console_level = logging.getLevelName(app_config.get_config('logging.console.level'))
        console_handler.setLevel(console_level)

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
