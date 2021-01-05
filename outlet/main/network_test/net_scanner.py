import logging

import nmap
import util.main_util

logger = logging.getLogger(__name__)


class NetworkScanner:
    def __init__(self):
        self.ip = None

    def scan(self):
        if self.ip:
            network = f'{self.ip}/24'
        else:
            network = '192.168.1.1/24'

        logger.info(f'Scanning...')

        nm = nmap.PortScanner()
        nm.scan(hosts=network, arguments='-sn')
        hosts_list = [(x, nm[x]['status']['state']) for x in nm.all_hosts()]
        for host, status in hosts_list:
            logger.info(f'Host:  {host}')


def main():
    config = util.main_util.do_main_boilerplate()
    scanner = NetworkScanner()
    scanner.scan()


if __name__ == '__main__':
    main()
