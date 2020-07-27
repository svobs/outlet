import unittest

from app_config import AppConfig
from outlet_app import OutletApplication


def fun(x):
    return x + 1


class ChangeTest(unittest.TestCase):
    def setUp(self) -> None:
        config = AppConfig()
        application = OutletApplication(config)
        # Disable execution so we can study the state of the OpTree:
        application.executor.enable_change_thread = False
        # this starts the executor, which inits the CacheManager
        application.start()

    def test(self):
        self.assertEqual(fun(3), 4)
