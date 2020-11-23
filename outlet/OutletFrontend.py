from typing import Dict, Optional

import logging

import ui.assets
from ui import actions
from ui.tree.controller import TreePanelController
from util.has_lifecycle import HasLifecycle

logger = logging.getLogger(__name__)


# CLASS OutletFrontend
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OutletFrontend(HasLifecycle):
    def __init__(self, config):
        HasLifecycle.__init__(self)
        self.assets = ui.assets.Assets(config)

        self._tree_controllers: Dict[str, TreePanelController] = {}
        """Keep track of live UI tree controllers, so that we can look them up by ID (e.g. for use in automated testing)"""

    def start(self):
        logger.debug('Starting up frontend')
        HasLifecycle.start(self)
        self.connect_dispatch_listener(signal=actions.DEREGISTER_DISPLAY_TREE, receiver=self._deregister_tree_controller)

    def shutdown(self):
        logger.debug('Shutting down frontend')
        HasLifecycle.shutdown(self)

        try:
            if self._tree_controllers:
                for controller in list(self._tree_controllers.values()):
                    controller.destroy()
                self._tree_controllers.clear()
        except NameError:
            pass

    # Tree controller tracking/lookup
    # ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

    def register_tree_controller(self, controller: TreePanelController):
        logger.debug(f'[{controller.tree_id}] Registering controller')
        self._tree_controllers[controller.tree_id] = controller

    def _deregister_tree_controller(self, sender: str):
        # Sender is tree_id
        logger.debug(f'[{sender}] Deregistering controller in frontend')
        popped_con = self._tree_controllers.pop(sender, None)
        if not popped_con:
            logger.debug(f'Could not deregister controller; it was not found: {sender}')

    def get_tree_controller(self, tree_id: str) -> Optional[TreePanelController]:
        return self._tree_controllers.get(tree_id, None)
