import logging
logger = logging.getLogger(__name__)


def configure_logging(config):
    # create logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # DEBUG LOG FILE
    debug_log_enabled = config.get('logging.debug_log.enable')
    if debug_log_enabled:
        debug_log_path = config.get('logging.debug_log.full_path')
        debug_log_mode = config.get('logging.debug_log.mode')
        debug_log_fmt = config.get('logging.debug_log.format')
        debug_log_datetime_fmt = config.get('logging.debug_log.datetime_format')

        debug_file_handler = logging.FileHandler(filename=debug_log_path, mode=debug_log_mode)
        debug_file_handler.setLevel(logging.DEBUG)

        debug_file_formatter = logging.Formatter(fmt=debug_log_fmt, datefmt=debug_log_datetime_fmt)
        debug_file_handler.setFormatter(debug_file_formatter)

        root_logger.addHandler(debug_file_handler)

    # CONSOLE
    console_enabled = config.get('logging.console.enable')
    if console_enabled:
        console_fmt = config.get('logging.debug_log.format')
        console_datetime_fmt = config.get('logging.debug_log.datetime_format')

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)

        console_formatter = logging.Formatter(fmt=console_fmt, datefmt=console_datetime_fmt)
        console_handler.setFormatter(console_formatter)

        # add console output to all loggers
        root_logger.addHandler(console_handler)

    info_loggers = config.get('logging.loglevel_info')
    for logger_name in info_loggers:
        logging.getLogger(logger_name).setLevel(logging.INFO)

    # --- Google API ---
    warning_loggers = config.get('logging.loglevel_warning')
    for logger_name in warning_loggers:
        logging.getLogger(logger_name).setLevel(logging.WARNING)
