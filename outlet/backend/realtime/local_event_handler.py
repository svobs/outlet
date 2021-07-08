from watchdog.events import FileSystemEventHandler
import logging

from model.node.local_disk_node import LocalDirNode, LocalNode

logger = logging.getLogger(__name__)


def _what(event):
    return 'dir' if event.is_directory else 'file'


class LocalChangeEventHandler(FileSystemEventHandler):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS LocalChangeEventHandler

    Logs all the events captured.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """
    def __init__(self, backend, batching_thread):
        super().__init__()
        self.backend = backend
        self.cacheman = self.backend.cacheman
        self.batching_thread = batching_thread

    def on_moved(self, event):
        try:
            super(LocalChangeEventHandler, self).on_moved(event)
            logger.info(f'Detected MV {_what(event)}: from "{event.src_path}" to "{event.dest_path}"')

            self.cacheman.move_local_subtree(event.src_path, event.dest_path, is_from_watchdog=True)
        except Exception:
            logger.exception(f'Error processing external event: MV {_what(event)} from "{event.src_path}" to "{event.dest_path}"')

    def on_created(self, event):
        try:
            super(LocalChangeEventHandler, self).on_created(event)
            logger.info(f'Detected MK {_what(event)}: {event.src_path}')

            if event.is_directory:
                node: LocalDirNode = self.cacheman.build_local_dir_node(event.src_path, is_live=True, all_children_fetched=True)
            else:
                node: LocalNode = self.cacheman.build_local_file_node(event.src_path)
            self.cacheman.upsert_single_node(node)
        except FileNotFoundError as err:
            logger.debug(f'Could not process external event (MK {_what(event)} "{event.src_path}"): file not found: "{err.filename}"')
        except Exception:
            logger.exception(f'Error processing external event: MK {_what(event)} "{event.src_path}"')

    def on_deleted(self, event):
        try:
            super(LocalChangeEventHandler, self).on_deleted(event)
            logger.info(f'Detected RM {_what(event)}: {event.src_path}')

            node: LocalNode = self.cacheman.get_node_for_local_path(event.src_path)
            if node:
                if node.is_dir():
                    assert event.is_directory, f'Not a directory: {event.src_path}'
                    self.cacheman.remove_subtree(node, to_trash=False)
                else:
                    self.cacheman.remove_node(node, to_trash=False)
            else:
                logger.debug(f'Cannot remove from cache: node not found in cache for path: {event.src_path}')
        except Exception:
            logger.exception(f'Error processing external event: RM {_what(event)} "{event.src_path}"')

    def on_modified(self, event):
        # When a watched file is modified, it hammers us with events. Wait a small interval between updates to avoid unnecessary CPU churn
        try:
            super(LocalChangeEventHandler, self).on_modified(event)
            # logger.debug(f'Detected CH {_what(event)}: {event.src_path}')

            # We don't currently track meta for local dirs
            if not event.is_directory:
                self.batching_thread.enqueue(event.src_path)
        except Exception:
            logger.exception(f'Error processing external event: CH {_what(event)} "{event.src_path}"')
