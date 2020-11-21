from typing import Dict, Optional

from pydispatch import dispatcher
import logging

import ui.assets
from ui import actions
from ui.tree.controller import TreePanelController

logger = logging.getLogger(__name__)


# CLASS OutletFrontend
# ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼

class OutletFrontend:
    def __init__(self, config):
        self.assets = ui.assets.Assets(config)

        self._tree_controllers: Dict[str, TreePanelController] = {}
        """Keep track of live UI tree controllers, so that we can look them up by ID (e.g. for use in automated testing)"""

    def start(self):
        logger.debug('Starting up frontend')

    def shutdown(self):
        logger.debug('Shutting down frontend')

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

    def unregister_tree_controller(self, controller: TreePanelController):
        logger.debug(f'[{controller.tree_id}] Unregistering controller')
        popped_con = self._tree_controllers.pop(controller.tree_id, None)
        if popped_con:
            # stop capturing if we have started:
            dispatcher.send(signal=actions.STOP_LIVE_CAPTURE, sender=controller.tree_id)
        else:
            logger.debug(f'Could not unregister TreeController; it was not found: {controller.tree_id}')

    def get_tree_controller(self, tree_id: str) -> Optional[TreePanelController]:
        return self._tree_controllers.get(tree_id, None)
