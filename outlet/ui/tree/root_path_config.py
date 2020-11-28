import logging

from model.node_identifier import ensure_bool, SinglePathNodeIdentifier
from util.root_path_meta import RootPathMeta

logger = logging.getLogger(__name__)


def make_tree_type_config_key(tree_id: str):
    return f'ui_state.{tree_id}.tree_type'


def make_root_path_config_key(tree_id: str):
    return f'ui_state.{tree_id}.root_path'


def make_root_uid_config_key(tree_id: str):
    return f'ui_state.{tree_id}.root_uid'


def make_root_exists_config_key(tree_id: str):
    return f'ui_state.{tree_id}.root_exists'


def make_root_offending_path_config_key(tree_id: str):
    return f'ui_state.{tree_id}.offending_path'


# CLASS RootPathConfigPersister
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class RootPathConfigPersister:
    """Reads and writes the root path for the given tree_id.
    Listens for signals to stay up-to-date. Configure this class, add it to its parent's instance variables, and then forget about it."""

    def __init__(self, backend, tree_id):
        self._tree_type_config_key = make_tree_type_config_key(tree_id)
        self._root_path_config_key = make_root_path_config_key(tree_id)
        self._root_uid_config_key = make_root_uid_config_key(tree_id)
        self._root_exists_config_key = make_root_exists_config_key(tree_id)
        self._root_offending_path_config_key = make_root_offending_path_config_key(tree_id)
        self._tree_id = tree_id
        self.backend = backend
        self._config = backend.config
        self.root_path_meta = None

    def read_from_config(self) -> RootPathMeta:
        logger.debug(f'Attempting to read root path info from config: {self._tree_id}')
        tree_type = self._config.get(self._tree_type_config_key)
        root_path = self._config.get(self._root_path_config_key)
        root_uid = self._config.get(self._root_uid_config_key)
        try:
            if not isinstance(root_uid, int):
                root_uid = int(root_uid)
        except ValueError:
            raise RuntimeError(f"Invalid value for tree's UID (expected integer): '{root_uid}'")

        root_identifier: SinglePathNodeIdentifier = self.backend.node_identifier_factory.for_values(tree_type=tree_type,
                                                                                                    path_list=root_path, uid=root_uid,
                                                                                                    must_be_single_path=True)

        root_exists = ensure_bool(self._config.get(self._root_exists_config_key))
        offending_path = self._config.get(self._root_offending_path_config_key)
        if offending_path == '':
            offending_path = None

        return RootPathMeta(root_identifier, root_exists=root_exists, offending_path=offending_path)

    def write_to_config(self, new_root_meta: RootPathMeta):
        if not self.root_path_meta or self.root_path_meta != new_root_meta:
            new_root = new_root_meta.root_spid
            logger.debug(f'Root path changed. Saving root to config: {self._tree_type_config_key} '
                         f'= {new_root.tree_type}, {self._root_path_config_key} = "{new_root.get_single_path()}", '
                         f'{self._root_uid_config_key} = "{new_root.uid}"')
            # Root changed. Invalidate the current tree contents
            self._config.write(json_path=self._tree_type_config_key, value=new_root.tree_type)
            self._config.write(json_path=self._root_path_config_key, value=new_root.get_single_path())
            self._config.write(json_path=self._root_uid_config_key, value=new_root.uid)
            self._config.write(json_path=self._root_exists_config_key, value=new_root_meta.root_exists)
            offending_path = new_root_meta.offending_path
            if not offending_path:
                offending_path = ''
            self._config.write(json_path=self._root_offending_path_config_key, value=offending_path)
        # always, just to be safe
        self.root_path_meta = new_root_meta
