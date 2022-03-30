import logging
from pathlib import PurePosixPath

from watchdog.events import FileSystemEventHandler

from logging_constants import SUPER_DEBUG_ENABLED, TRACE_ENABLED

logger = logging.getLogger(__name__)


def _what(event):
    return 'dir' if event.is_directory else 'file'


class LocalChangeEventHandler(FileSystemEventHandler):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalChangeEventHandler

    Logs all the events captured for a given file tree of a given path.
    Multiple LocalChangeEventHandlers will report to the LocalFileChangeBatchingThread, which will merge the updates.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, batching_thread):
        super().__init__()
        self.backend = backend
        self.cacheman = self.backend.cacheman
        self.batching_thread = batching_thread
        self.project_dir = self.backend.get_project_dir()

    def on_moved(self, event):
        try:
            if PurePosixPath(event.src_path).is_relative_to(self.project_dir) and PurePosixPath(event.dest_path).is_relative_to(self.project_dir):
                if TRACE_ENABLED:
                    logger.debug(f'Skipping (is descendant of proj dir): MV {_what(event)}: from "{event.src_path}" to "{event.dest_path}"')
                return
            super(LocalChangeEventHandler, self).on_moved(event)
            logger.info(f'Detected MV {_what(event)}: from "{event.src_path}" to "{event.dest_path}"')

            self.batching_thread.enqueue_move(event.src_path, event.dest_path)

        except Exception:
            logger.exception(f'Error processing external event: MV {_what(event)} from "{event.src_path}" to "{event.dest_path}"')

    def on_created(self, event):
        try:
            if PurePosixPath(event.src_path).is_relative_to(self.project_dir):
                if TRACE_ENABLED:
                    logger.debug(f'Skipping (is descendant of proj dir): MK {_what(event)} "{event.src_path}"')
                return
            super(LocalChangeEventHandler, self).on_created(event)
            logger.info(f'Detected MK {_what(event)} "{event.src_path}"')

            self.batching_thread.enqueue_create(event.src_path)

        # TODO: is this exception actually raised? Comment out and see
        # except FileNotFoundError as err:
        #     logger.debug(f'Could not process external event (MK {_what(event)} "{event.src_path}"): file not found: "{err.filename}"')
        except Exception:
            logger.exception(f'Error processing external event: MK {_what(event)} "{event.src_path}"')

    def on_deleted(self, event):
        try:
            if PurePosixPath(event.src_path).is_relative_to(self.project_dir):
                if TRACE_ENABLED:
                    logger.debug(f'Skipping (is descendant of proj dir): RM {_what(event)} "{event.src_path}"')
                return
            super(LocalChangeEventHandler, self).on_deleted(event)
            logger.info(f'Detected RM {_what(event)} "{event.src_path}"')

            self.batching_thread.enqueue_delete(event.src_path)

        except Exception:
            logger.exception(f'Error processing external event: RM {_what(event)} "{event.src_path}"')

    def on_modified(self, event):
        # When a watched file is modified, it hammers us with events. Wait a small interval between updates to avoid unnecessary CPU churn
        try:
            if PurePosixPath(event.src_path).is_relative_to(self.project_dir):
                if TRACE_ENABLED:
                    logger.debug(f'Skipping (is descendant of proj dir): CH {_what(event)} "{event.src_path}"')
                return
            super(LocalChangeEventHandler, self).on_modified(event)
            # logger.debug(f'Detected CH {_what(event)}: {event.src_path}')

            # We don't currently track meta for local dirs
            if not event.is_directory:
                self.batching_thread.enqueue_modify(event.src_path)
        except Exception:
            logger.exception(f'Error processing external event: CH {_what(event)} "{event.src_path}"')
