from pydispatch import dispatcher

from ui import actions


class RootPathConfigPersister:
    """Reads and writes the root path for the given tree_id.
    Listens for signals to stay up-to-date. Configure this class, add it to its
    parent's instance variables, and then forget about it."""
    def __init__(self, config, tree_id):
        self._config_key = f'transient.{tree_id}.root_path'
        self._config = config
        self.root_path = self._config.get(self._config_key)

        dispatcher.connect(signal=actions.ROOT_PATH_UPDATED, receiver=self._on_root_path_updated, sender=tree_id)

    def _on_root_path_updated(self, sender, new_root):
        if self.root_path != new_root:
            # Root changed. Invalidate the current tree contents
            self._config.write(transient_path=self._config_key, value=new_root)
        # always, just to be safe
        self.root_path = new_root
