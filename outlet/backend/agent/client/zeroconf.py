import logging
import socket
import threading
from typing import Optional

from zeroconf import ServiceBrowser, ServiceInfo, ServiceListener, Zeroconf

from constants import ZEROCONF_SERVICE_TYPE

logger = logging.getLogger(__name__)


class OutletZeroconfListener(ServiceListener):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS OutletZeroconfListener
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, grpc_client):
        self.zeroconf: Optional[Zeroconf] = None
        self.grpc_client = grpc_client
        self.connected_successfully = threading.Event()

    def __enter__(self):
        self.zeroconf = Zeroconf()
        ServiceBrowser(self.zeroconf, ZEROCONF_SERVICE_TYPE, self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.zeroconf.close()

    def wait_for_successful_connect(self, timeout_sec: int) -> bool:
        if self.connected_successfully.is_set():
            return True
        else:
            return self.connected_successfully.wait(timeout_sec)

    def remove_service(self, zc: 'Zeroconf', type_: str, name: str) -> None:
        logger.info(f'Service "{name}" removed')

    def add_service(self, zc: 'Zeroconf', type_: str, name: str) -> None:
        logger.info(f'Service "{name}" added: type={type}')
        timeout_ms = 3000
        info: ServiceInfo = self.zeroconf.get_service_info(type_, name, timeout_ms)
        if not info:
            raise RuntimeError(f'Failed to get service info for {type_}')

        address_list = []
        for index, address in enumerate(info.addresses):
            address = socket.inet_ntoa(address)
            port = info.port
            address_list.append((address, port))

            logger.debug(f'  Address[{index}]: "{address}:{port}", weight={info.weight}, priority={info.priority}, server="{info.server}",'
                         f'properties={info.properties}')

        for address, port in address_list:
            try:
                self.grpc_client.connect(address, port)
                logger.debug(f'Looks like connection to {address}:{port} was successful!')

                self.connected_successfully.set()
                return
            except RuntimeError as e:
                logger.error(f'Failed to connect to {address}:{port}: {repr(e)}')

    def update_service(self, zc: 'Zeroconf', type_: str, name: str) -> None:
        pass
