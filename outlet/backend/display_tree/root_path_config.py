import logging

from constants import TreeID, UI_STATE_CFG_SEGMENT
from model.node_identifier import SinglePathNodeIdentifier
from util.ensure import ensure_bool, ensure_uid
from util.root_path_meta import RootPathMeta

logger = logging.getLogger(__name__)


def make_device_uid_config_key(tree_id: TreeID):
    return f'{UI_STATE_CFG_SEGMENT}.{tree_id}.root_device_uid'


def make_root_path_config_key(tree_id: TreeID):
    return f'{UI_STATE_CFG_SEGMENT}.{tree_id}.root_path'


def make_root_uid_config_key(tree_id: TreeID):
    return f'{UI_STATE_CFG_SEGMENT}.{tree_id}.root_uid'


def make_root_exists_config_key(tree_id: TreeID):
    return f'{UI_STATE_CFG_SEGMENT}.{tree_id}.root_exists'


def make_root_offending_path_config_key(tree_id: TreeID):
    return f'{UI_STATE_CFG_SEGMENT}.{tree_id}.offending_path'


class RootPathConfigPersister:
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS RootPathConfigPersister

    Reads and writes the root path for the given tree_id.
    Listens for signals to stay up-to-date. Configure this class, add it to its parent's instance variables, and then forget about it.
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, backend, tree_id):
        self._device_uid_config_key = make_device_uid_config_key(tree_id)
        self._root_path_config_key = make_root_path_config_key(tree_id)
        self._root_uid_config_key = make_root_uid_config_key(tree_id)
        self._root_exists_config_key = make_root_exists_config_key(tree_id)
        self._root_offending_path_config_key = make_root_offending_path_config_key(tree_id)
        self._tree_id = tree_id
        self.backend = backend
        self.root_path_meta = None

    def read_from_config(self) -> RootPathMeta:
        logger.debug(f'[{self._tree_id}] Attempting to read root path info from app_config')
        device_uid = ensure_uid(self.backend.get_config(self._device_uid_config_key, required=True))
        root_path = self.backend.get_config(self._root_path_config_key, required=True)
        root_uid = ensure_uid(self.backend.get_config(self._root_uid_config_key, required=True))

        root_identifier: SinglePathNodeIdentifier = self.backend.node_identifier_factory.build_spid(node_uid=root_uid, device_uid=device_uid,
                                                                                                    single_path=root_path)

        root_exists = ensure_bool(self.backend.get_config(self._root_exists_config_key))
        offending_path = self.backend.get_config(self._root_offending_path_config_key)
        if offending_path == '':
            offending_path = None

        meta = RootPathMeta(root_identifier, root_exists=root_exists, offending_path=offending_path)
        logger.debug(f'Read root path meta from config: {meta}')
        return meta

    def write_to_config(self, new_root_meta: RootPathMeta):
        if not self.root_path_meta or self.root_path_meta != new_root_meta:
            new_root = new_root_meta.root_spid
            logger.debug(f'[{self._tree_id}] Root path changed. Saving root to app_config: {self._device_uid_config_key} '
                         f'= {new_root.device_uid}, {self._root_path_config_key} = "{new_root.get_single_path()}", '
                         f'{self._root_uid_config_key} = "{new_root.node_uid}"')
            # Root changed. Invalidate the current tree contents
            self.backend.put_config(config_key=self._device_uid_config_key, config_val=new_root.device_uid)
            self.backend.put_config(config_key=self._root_path_config_key, config_val=new_root.get_single_path())
            self.backend.put_config(config_key=self._root_uid_config_key, config_val=new_root.node_uid)
            self.backend.put_config(config_key=self._root_exists_config_key, config_val=new_root_meta.root_exists)
            offending_path = new_root_meta.offending_path
            if not offending_path:
                offending_path = ''
            self.backend.put_config(config_key=self._root_offending_path_config_key, config_val=offending_path)
        # always, just to be safe
        self.root_path_meta = new_root_meta
