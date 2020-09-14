from watchdog.events import FileSystemEventHandler
import logging

logger = logging.getLogger(__name__)


class LocalChangeEventHandler(FileSystemEventHandler):
    """Logs all the events captured."""

    def on_moved(self, event):
        super(LocalChangeEventHandler, self).on_moved(event)

        what = 'directory' if event.is_directory else 'file'
        logger.info("Moved %s: from %s to %s", what, event.src_path,
                     event.dest_path)

    def on_created(self, event):
        super(LocalChangeEventHandler, self).on_created(event)

        what = 'directory' if event.is_directory else 'file'
        logger.info("Created %s: %s", what, event.src_path)

    def on_deleted(self, event):
        super(LocalChangeEventHandler, self).on_deleted(event)

        what = 'directory' if event.is_directory else 'file'
        logger.info("Deleted %s: %s", what, event.src_path)

    def on_modified(self, event):
        super(LocalChangeEventHandler, self).on_modified(event)

        what = 'directory' if event.is_directory else 'file'
        logger.info("Modified %s: %s", what, event.src_path)
