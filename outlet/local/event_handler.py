from watchdog.events import FileSystemEventHandler
import logging

from model.node.local_disk_node import LocalDirNode, LocalNode

logger = logging.getLogger(__name__)


def _what(event):
    return 'directory' if event.is_directory else 'file'


# CLASS LocalChangeEventHandler
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class LocalChangeEventHandler(FileSystemEventHandler):
    """Logs all the events captured."""
    def __init__(self, application):
        super().__init__()
        self.app = application
        self.cacheman = self.app.cache_manager

    def on_moved(self, event):
        super(LocalChangeEventHandler, self).on_moved(event)
        logger.info(f'Moved {_what(event)}: from {event.src_path} to {event.dest_path}')

        # FIXME
        node_before_move: LocalNode = self.cacheman.get_node_for_local_path(event.src_path)
        if node_before_move:
            self.cacheman.remove_node(node_before_move, to_trash=False)
        else:
            logger.debug(f'Cannot remove moved src node from cache: node not found in cache for path: {event.src_path}')

        node: LocalNode = self.cacheman.build_local_file_node(event.dest_path)
        self.cacheman.add_or_update_node(node)

    def on_created(self, event):
        super(LocalChangeEventHandler, self).on_created(event)
        logger.info(f'Created {_what(event)}: {event.src_path}')

        if event.is_directory:
            node: LocalDirNode = self.cacheman.build_local_dir_node(event.src_path)
        else:
            node: LocalNode = self.cacheman.build_local_file_node(event.src_path)
        self.cacheman.add_or_update_node(node)

    def on_deleted(self, event):
        super(LocalChangeEventHandler, self).on_deleted(event)
        logger.info(f'Deleted {_what(event)}: {event.src_path}')

        node: LocalNode = self.cacheman.get_node_for_local_path(event.src_path)
        if node:
            if node.is_dir():
                assert event.is_directory, f'Not a directory: {event.src_path}'
                self.cacheman.remove_subtree(node, to_trash=False)
            else:
                self.cacheman.remove_node(node, to_trash=False)
        else:
            logger.debug(f'Cannot remove from cache: node not found in cache for path: {event.src_path}')

    def on_modified(self, event):
        super(LocalChangeEventHandler, self).on_modified(event)
        logger.info(f'Modified {_what(event)}: {event.src_path}')

        # We don't currently track meta for local dirs
        if not event.is_directory:
            node: LocalNode = self.cacheman.build_local_file_node(event.src_path)
            self.cacheman.add_or_update_node(node)