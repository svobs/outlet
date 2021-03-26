from typing import Dict

from zeroconf import *
import logging

import socket
import util.main_util

logger = logging.getLogger(__name__)


class MyListener(ServiceListener):
    def __init__(self, zeroconf):
        self.zeroconf = zeroconf

    def remove_service(self, zc: 'Zeroconf', type_: str, name: str) -> None:
        logger.info(f'Service {name} removed')

    def add_service(self, zc: 'Zeroconf', type_: str, name: str) -> None:
        logger.info(f'Service {name} added')
        logger.info(f'  Type is {type_}')
        timeout_ms = 3000
        info: ServiceInfo = self.zeroconf.get_service_info(type_, name, timeout_ms)
        if info:
            for address in info.addresses:
                logger.info(f'  Address is {socket.inet_ntoa(address)}:{info.port}')
                logger.info(f'  Weight is {info.weight}, Priority is {info.priority}')
                logger.info(f'  Server is {info.server}')
                prop_dict: Dict = info.properties
                if prop_dict:
                    logger.info("  Properties are: ")
                    for key, value in prop_dict.items():
                        logger.info(f"    {key}: {value}")

    def update_service(self, zc: 'Zeroconf', type_: str, name: str) -> None:
        pass


def main():
    app_config = util.main_util.do_main_boilerplate(executing_script_path=__file__)

    service_type = "_outlet._tcp.local."
    service_type = '_discoverable._udp.local.'#, name = 'Matt-Mac Service._discoverable._udp.local.',

    zeroconf = Zeroconf()
    try:
        listener = MyListener(zeroconf)
        browser = ServiceBrowser(zeroconf, service_type, listener)
        # Search for devices for 40 seconds.
        input("Press enter to exit...\n\n")
    finally:
        zeroconf.close()


if __name__ == '__main__':
    main()
