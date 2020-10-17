from app_config import AppConfig
from store.gdrive.client import GDriveClient
import logging
from outlet_app import OutletApplication

logger = logging.getLogger(__name__)


def main():
    config = AppConfig()

    app = OutletApplication(config)
    client = GDriveClient(app)
    result = client.get_all_shared_with_me()

    logger.debug('Done!')


if __name__ == '__main__':
    main()
