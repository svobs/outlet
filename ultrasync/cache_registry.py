import logging
import os
from file_util import get_resource_path

MAIN_REGISTRY_FILE_NAME = 'registry.db'

logger = logging.getLogger(__name__)


class CacheRegistry:
    def __init__(self, config):
        self.config = config
        self.cache_dir_path = get_resource_path(self.config.get('cache_dir_path'))
        self.main_registry_path = os.path.join(self.cache_dir_path, MAIN_REGISTRY_FILE_NAME)

    def get_cache_info(self):
        if not os.path.exists(self.main_registry_path):
            logger.info(f'Main registry does not exist; attempting to create: "{self.main_registry_path}"')
            os.makedirs(name=self.cache_dir_path, exist_ok=True)
            # main_registry = MetaDatabase(self.main_registry_path)

