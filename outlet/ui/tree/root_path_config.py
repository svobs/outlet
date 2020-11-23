import logging

from pydispatch import dispatcher

from constants import TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from model.node.node import Node
from store.uid.uid_generator import NULL_UID
from model.node_identifier import NodeIdentifier, SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from ui import actions

logger = logging.getLogger(__name__)


def make_tree_type_config_key(tree_id: str):
    return f'ui_state.{tree_id}.tree_type'


def make_root_path_config_key(tree_id: str):
    return f'ui_state.{tree_id}.root_path'


def make_root_uid_config_key(tree_id: str):
    return f'ui_state.{tree_id}.root_uid'


# CLASS RootPathConfigPersister
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class RootPathConfigPersister:
    """Reads and writes the root path for the given tree_id.
    Listens for signals to stay up-to-date. Configure this class, add it to its
    parent's instance variables, and then forget about it."""

    def __init__(self, app, tree_id):
        self._tree_type_config_key = make_tree_type_config_key(tree_id)
        self._root_path_config_key = make_root_path_config_key(tree_id)
        self._root_uid_config_key = make_root_uid_config_key(tree_id)
        self._tree_id = tree_id
        self.app = app
        self._config = app.config
        tree_type = self._config.get(self._tree_type_config_key)
        root_path = self._config.get(self._root_path_config_key)
        root_uid = self._config.get(self._root_uid_config_key)
        try:
            if not isinstance(root_uid, int):
                root_uid = int(root_uid)
        except ValueError:
            raise RuntimeError(f"Invalid value for tree's UID (expected integer): '{root_uid}'")

        # Cross-check that our local root UIDs are correct:
        if tree_type == TREE_TYPE_LOCAL_DISK:
            # resolves a problem of inconsistent UIDs during testing
            node: Node = self.app.backend.read_single_node_from_disk_for_path(root_path, tree_type)
            if node:
                if root_uid != node.uid:
                    logger.warning(f'UID from config ({root_uid}) does not match UID from cache ({node.uid}); will use value from cache')
                    root_uid = node.uid
            else:
                # TODO: make into RPC call
                new_root, err = self.app.cacheman.resolve_root_from_path(root_path)
                root_uid = new_root.uid
                self.root_identifier = new_root
                logger.info(f'[{tree_id}] Sending signal: "{actions.ROOT_PATH_UPDATED}" with new_root={new_root}, err={err}')
                dispatcher.send(signal=actions.ROOT_PATH_UPDATED, sender=tree_id, new_root=new_root, err=err)

        self.root_identifier: SinglePathNodeIdentifier = self.app.backend.build_identifier(tree_type=tree_type,
                                                                                           path_list=root_path, uid=root_uid,
                                                                                           must_be_single_path=True)

        dispatcher.connect(signal=actions.ROOT_PATH_UPDATED, receiver=self._on_root_path_updated, sender=tree_id)
        dispatcher.connect(signal=actions.GDRIVE_RELOADED, receiver=self._on_gdrive_reloaded)

    def _on_root_path_updated(self, sender: str, new_root: SinglePathNodeIdentifier, err=None):
        logger.info(f'Received signal: "{actions.ROOT_PATH_UPDATED}" with root: {new_root}, err: {err}')
        if self.root_identifier != new_root:
            logger.debug(f'Root path changed. Saving root to config: {self._tree_type_config_key} '
                         f'= {new_root.tree_type}, {self._root_path_config_key} = "{new_root.get_single_path()}", '
                         f'{self._root_uid_config_key} = "{new_root.uid}"')
            # Root changed. Invalidate the current tree contents
            self._config.write(json_path=self._tree_type_config_key, value=new_root.tree_type)
            self._config.write(json_path=self._root_path_config_key, value=new_root.get_single_path())
            if err:
                self._config.write(json_path=self._root_uid_config_key, value=NULL_UID)
            else:
                self._config.write(json_path=self._root_uid_config_key, value=new_root.uid)
        # always, just to be safe
        self.root_identifier = new_root

    def _on_gdrive_reloaded(self, sender: str):
        logger.info(f'Received signal: "{actions.GDRIVE_RELOADED}"')
        if self.root_identifier.tree_type == TREE_TYPE_GDRIVE:
            # If GDrive was reloaded, our previous selection was almost certainly invalid. Just reset to GDrive root.
            new_root = NodeIdentifierFactory.get_gdrive_root_constant_identifier()
            if new_root != self.root_identifier:
                self.root_identifier = new_root
                err = None
                logger.info(f'[{self._tree_id}] Sending signal: "{actions.ROOT_PATH_UPDATED}" with new_root={new_root}, err={err}')
                dispatcher.send(signal=actions.ROOT_PATH_UPDATED, sender=self._tree_id, new_root=new_root, err=err)
