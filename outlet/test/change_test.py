import threading
import time
import unittest
import logging

from pydispatch import dispatcher

from app_config import AppConfig
from outlet_app import OutletApplication
from ui import actions
from ui.actions import DRAG_AND_DROP_DIRECT
from ui.two_pane_window import TwoPanelWindow

logger = logging.getLogger(__name__)


class ChangeTest(unittest.TestCase):
    def setUp(self) -> None:
        config = AppConfig()
        application = OutletApplication(config)
        # Disable execution so we can study the state of the OpTree:
        application.executor.enable_change_thread = False

        load_left_done = threading.Event()
        load_right_done = threading.Event()

        def run_thread():
            # this starts the executor, which inits the CacheManager
            # This does not return until the program exits
            application.run([])

        def after_left_tree_loaded(sender):
            logger.debug(f'Received signal: "{actions.LOAD_UI_TREE}" for "{sender}"')
            load_left_done.set()

        def after_right_tree_loaded(sender):
            logger.debug(f'Received signal: "{actions.LOAD_UI_TREE}" for "{sender}"')
            load_right_done.set()

        dispatcher.connect(signal=actions.LOAD_UI_TREE, sender=actions.ID_LEFT_TREE, receiver=after_left_tree_loaded)
        dispatcher.connect(signal=actions.LOAD_UI_TREE, sender=actions.ID_RIGHT_TREE, receiver=after_right_tree_loaded)
        thread = threading.Thread(target=run_thread, daemon=True)
        thread.start()

        # wait for both sides to load before returning:
        if not load_left_done.wait(10):
            raise RuntimeError('Timed out waiting for left to load!')
        if not load_right_done.wait(10):
            raise RuntimeError('Timed out waiting for right to load!')
        logger.info(f'LOAD COMPLETE')

    def test(self):
        logger.info('Doing drop test')
        dispatcher.send(signal=DRAG_AND_DROP_DIRECT, sender=actions.ID_LEFT_TREE, drag_data=None, tree_path=None, is_into=False)
        logger.info('Sleeping')
        time.sleep(10) # in seconds
        logger.info('Done!')
