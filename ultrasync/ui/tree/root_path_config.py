from pydispatch import dispatcher
import logging

from constants import NULL_UID
from model.node_identifier import NodeIdentifier, NodeIdentifierFactory
from ui import actions

logger = logging.getLogger(__name__)


class RootPathConfigPersister:
    """Reads and writes the root path for the given tree_id.
    Listens for signals to stay up-to-date. Configure this class, add it to its
    parent's instance variables, and then forget about it."""

    def __init__(self, config, tree_id):
        self._tree_type_config_key = f'transient.{tree_id}.tree_type'
        self._root_path_config_key = f'transient.{tree_id}.root_path'
        self._root_uid_config_key = f'transient.{tree_id}.root_uid'
        self._config = config
        tree_type = self._config.get(self._tree_type_config_key)
        root_path = self._config.get(self._root_path_config_key)
        root_uid = self._config.get(self._root_uid_config_key)
        if root_uid == NULL_UID:
            root_uid = None
        self.root_identifier = NodeIdentifierFactory.for_values(tree_type=tree_type, full_path=root_path, uid=root_uid)

        dispatcher.connect(signal=actions.ROOT_PATH_UPDATED, receiver=self._on_root_path_updated, sender=tree_id)

    def _on_root_path_updated(self, sender: str, new_root: NodeIdentifier, err=None):
        logger.info(f'Received signal: "{actions.ROOT_PATH_UPDATED}" with root: {new_root}')
        if self.root_identifier != new_root:
            logger.debug(f'Root path changed. Saving root to config: {self._tree_type_config_key} '
                         f'= {new_root.tree_type}, {self._root_path_config_key} = "{new_root.full_path}", '
                         f'{self._root_uid_config_key} = "{new_root.uid}"')
            # Root changed. Invalidate the current tree contents
            self._config.write(transient_path=self._tree_type_config_key, value=new_root.tree_type)
            self._config.write(transient_path=self._root_path_config_key, value=new_root.full_path)
            if err:
                self._config.write(transient_path=self._root_uid_config_key, value=NULL_UID)
            else:
                self._config.write(transient_path=self._root_uid_config_key, value=new_root.uid)
        # always, just to be safe
        self.root_identifier = new_root
