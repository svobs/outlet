import logging
logger = logging.getLogger(__name__)


def configure_logging(config):
    # create logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # DEBUG LOG FILE
    debug_file_handler = None
    debug_log_enabled = config.get('logging.debug_log.enable')
    if debug_log_enabled:
        debug_log_path = config.get('logging.debug_log.file_path')
        debug_log_mode = config.get('logging.debug_log.mode')
        debug_log_fmt = config.get('logging.debug_log.format')
        debug_log_datetime_fmt = config.get('logging.debug_log.datetime_format')

        debug_file_handler = logging.FileHandler(filename=debug_log_path, mode=debug_log_mode)
        debug_file_handler.setLevel(logging.DEBUG)

        debug_file_formatter = logging.Formatter(fmt=debug_log_fmt, datefmt=debug_log_datetime_fmt)
        debug_file_handler.setFormatter(debug_file_formatter)

        root_logger.addHandler(debug_file_handler)

    # CONSOLE
    console_handler = None
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

    # TODO: figure out how to externalize this
    logging.getLogger('fmeta.fmeta').setLevel(logging.INFO)
    logging.getLogger('fmeta.diff_content_first').setLevel(logging.INFO)
    logging.getLogger('ui.tree.display_store').setLevel(logging.INFO)

    # --- Google API ---
    # Set to INFO or loggier to go back to logging Google API request URLs
    # TODO: how the hell do I log this to a separate file??
    logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)
