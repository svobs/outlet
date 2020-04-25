import logging
import os

from cache.cache_registry_db import CacheRegistry
from file_util import get_resource_path

MAIN_REGISTRY_FILE_NAME = 'registry.db'


logger = logging.getLogger(__name__)


def _ensure_cache_dir_path(config):
    cache_dir_path = get_resource_path(config.get('cache_dir_path'))
    if not os.path.exists(cache_dir_path):
        logger.info(f'Cache directory does not exist; attempting to create: "{cache_dir_path}"')
    os.makedirs(name=cache_dir_path, exist_ok=True)
    return cache_dir_path


class CacheManager:
    def __init__(self, application, config):
        self.application = application
        self.config = config

        self.cache_dir_path = _ensure_cache_dir_path(config)

        self.main_registry_path = os.path.join(self.cache_dir_path, MAIN_REGISTRY_FILE_NAME)
        self.cache_registry_db = CacheRegistry(self.main_registry_path)


