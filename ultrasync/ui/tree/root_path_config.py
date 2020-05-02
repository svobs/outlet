from pydispatch import dispatcher
import logging

from ui import actions

logger = logging.getLogger(__name__)


class RootPathConfigPersister:
    """Reads and writes the root path for the given tree_id.
    Listens for signals to stay up-to-date. Configure this class, add it to its
    parent's instance variables, and then forget about it."""
    def __init__(self, config, tree_id):
        self._tree_type_config_key = f'transient.{tree_id}.tree_type'
        self._root_config_key = f'transient.{tree_id}.root_path'
        self._config = config
        self.tree_type = self._config.get(self._tree_type_config_key)
        self.root_path = self._config.get(self._root_config_key)

        dispatcher.connect(signal=actions.ROOT_PATH_UPDATED, receiver=self._on_root_path_updated, sender=tree_id)

    def _on_root_path_updated(self, sender, new_root, tree_type):
        logger.info(f'Received signal: {actions.ROOT_PATH_UPDATED} with root: {new_root}')
        if self.root_path != new_root:
            logger.debug(f'Root path changed. Saving to config: {self._tree_type_config_key} = {tree_type}, {self._root_config_key} = "{new_root}"')
            # Root changed. Invalidate the current tree contents
            self._config.write(transient_path=self._tree_type_config_key, value=tree_type)
            self._config.write(transient_path=self._root_config_key, value=new_root)
        # always, just to be safe
        self.root_path = new_root
