import logging
import os

from pydispatch import dispatcher

from constants import TREE_TYPE_GDRIVE, TREE_TYPE_LOCAL_DISK
from model.node.node import Node
from store.uid.uid_generator import NULL_UID
from model.node_identifier import ensure_bool, NodeIdentifier, SinglePathNodeIdentifier
from model.node_identifier_factory import NodeIdentifierFactory
from ui import actions
from util.has_lifecycle import HasLifecycle
from util.root_path_meta import RootPathMeta

logger = logging.getLogger(__name__)


def make_tree_type_config_key(tree_id: str):
    return f'ui_state.{tree_id}.tree_type'


def make_root_path_config_key(tree_id: str):
    return f'ui_state.{tree_id}.root_path'


def make_root_uid_config_key(tree_id: str):
    return f'ui_state.{tree_id}.root_uid'


def make_root_is_found_config_key(tree_id: str):
    return f'ui_state.{tree_id}.is_found'


def make_root_offending_path_config_key(tree_id: str):
    return f'ui_state.{tree_id}.offending_path'


# CLASS RootPathConfigPersister
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class RootPathConfigPersister(HasLifecycle):
    """Reads and writes the root path for the given tree_id.
    Listens for signals to stay up-to-date. Configure this class, add it to its parent's instance variables, and then forget about it."""

    def __init__(self, app, tree_id):
        HasLifecycle.__init__(self)
        self._tree_type_config_key = make_tree_type_config_key(tree_id)
        self._root_path_config_key = make_root_path_config_key(tree_id)
        self._root_uid_config_key = make_root_uid_config_key(tree_id)
        self._root_is_found_config_key = make_root_is_found_config_key(tree_id)
        self._root_offending_path_config_key = make_root_offending_path_config_key(tree_id)
        self._tree_id = tree_id
        self.app = app
        self._config = app.config
        self.root_path_meta: RootPathMeta = self._read_from_config()

        # Cross-check that our local root UIDs are correct.
        # (We do this because we can do it quickly; inversely for GDrive, we skip GDrive validation because it takes too long)
        tree_type = self.root_path_meta.root.tree_type
        root_path = self.root_path_meta.root.get_single_path()
        if tree_type == TREE_TYPE_LOCAL_DISK:
            # FIXME: this first nasty block can go away after we implement the fix noted at CacheManager.get_uid_for_path()
            # resolves a problem of inconsistent UIDs during testing
            node: Node = self.app.backend.read_single_node_from_disk_for_path(root_path, tree_type)
            if node:
                if self.root_path_meta.root.uid != node.uid:
                    logger.warning(f'UID from config ({self.root_path_meta.root.uid}) does not match UID from cache '
                                   f'({node.uid}); will use value from cache')
                self.root_path_meta.root = node.node_identifier
            else:
                # TODO: make into RPC call
                self.root_path_meta: RootPathMeta = self.app.backend.resolve_root_from_path(root_path)
                self.root_identifier = self.root_path_meta.root
                logger.info(f'[{tree_id}] Sending signal: "{actions.ROOT_PATH_UPDATED}" with new_root_meta={self.root_path_meta}')
                dispatcher.send(signal=actions.ROOT_PATH_UPDATED, sender=tree_id, new_root_meta=self.root_path_meta)

            if os.path.exists(root_path):
                # Override in case something changed since the last shutdown
                self.root_path_meta.is_found = True
                self.root_path_meta.offending_path = None
            else:
                self.root_path_meta.is_found = False

        self.start()

    def start(self):
        HasLifecycle.start(self)
        # Start listeners. These will be auto-disconnected when the parent object is shut down (at __del__())
        self.connect_dispatch_listener(signal=actions.ROOT_PATH_UPDATED, receiver=self._on_root_path_updated, sender=self._tree_id)
        self.connect_dispatch_listener(signal=actions.GDRIVE_RELOADED, receiver=self._on_gdrive_reloaded)

    def _read_from_config(self) -> RootPathMeta:
        tree_type = self._config.get(self._tree_type_config_key)
        root_path = self._config.get(self._root_path_config_key)
        root_uid = self._config.get(self._root_uid_config_key)
        try:
            if not isinstance(root_uid, int):
                root_uid = int(root_uid)
        except ValueError:
            raise RuntimeError(f"Invalid value for tree's UID (expected integer): '{root_uid}'")

        root_identifier: SinglePathNodeIdentifier = self.app.backend.node_identifier_factory.for_values(tree_type=tree_type,
                                                                                                        path_list=root_path, uid=root_uid,
                                                                                                        must_be_single_path=True)

        is_found = ensure_bool(self._config.get(self._root_is_found_config_key))
        offending_path = self._config.get(self._root_offending_path_config_key)
        if offending_path == '':
            offending_path = None

        return RootPathMeta(root_identifier, is_found=is_found, offending_path=offending_path)

    def _on_root_path_updated(self, sender: str, new_root_meta: RootPathMeta):
        logger.info(f'Received signal: "{actions.ROOT_PATH_UPDATED}" with new_root_meta: {new_root_meta}')
        if self.root_path_meta != new_root_meta:
            new_root = new_root_meta.root
            logger.debug(f'Root path changed. Saving root to config: {self._tree_type_config_key} '
                         f'= {new_root.tree_type}, {self._root_path_config_key} = "{new_root.get_single_path()}", '
                         f'{self._root_uid_config_key} = "{new_root.uid}"')
            # Root changed. Invalidate the current tree contents
            self._config.write(json_path=self._tree_type_config_key, value=new_root.tree_type)
            self._config.write(json_path=self._root_path_config_key, value=new_root.get_single_path())
            self._config.write(json_path=self._root_uid_config_key, value=new_root.uid)
            self._config.write(json_path=self._root_is_found_config_key, value=new_root_meta.is_found)
            offending_path = new_root_meta.offending_path
            if not offending_path:
                offending_path = ''
            self._config.write(json_path=self._root_offending_path_config_key, value=offending_path)
        # always, just to be safe
        self.root_path_meta = new_root_meta

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
