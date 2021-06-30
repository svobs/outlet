import logging
import socket
from concurrent import futures
from typing import List

import grpc
import netifaces
from zeroconf import ServiceInfo, Zeroconf

from backend.backend_integrated import BackendIntegrated
from backend.agent.grpc.generated import Outlet_pb2_grpc
from backend.agent.server.grpc_service import OutletGRPCService
from constants import GRPC_SERVER_MAX_WORKER_THREADS, LOOPBACK_ADDRESS, ZEROCONF_SERVICE_NAME, ZEROCONF_SERVICE_TYPE, ZEROCONF_SERVICE_VERSION
from util.ensure import ensure_bool, ensure_int

logger = logging.getLogger(__name__)


class OutletAgent(BackendIntegrated):
    """
    ▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼▼
    CLASS OutletAgent
    ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼
    """

    def __init__(self, config):
        BackendIntegrated.__init__(self, config)

        self._grpc_service = OutletGRPCService(self)
        """Contains the gRPC client code"""

        self.use_zeroconf: bool = not ensure_bool(self.get_config('agent.grpc.use_fixed_address'))
        self.zeroconf = None
        self.local_ip = None
        self.zc_info = None

    def start(self):
        self._grpc_service.start()
        BackendIntegrated.start(self)

    def shutdown(self):
        BackendIntegrated.shutdown(self)
        self.unregister_zeroconf()
        try:
            self._grpc_service.shutdown()
        except AttributeError:
            pass

    def serve(self):
        # See note about GRPC_SERVER_MAX_WORKER_THREADS
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=GRPC_SERVER_MAX_WORKER_THREADS), options=(
                                                            ('grpc.keepalive_time_ms', 10000),
                                                            # send keepalive ping every 10 seconds, default is 2 hours
                                                            ('grpc.keepalive_timeout_ms', 5000),
                                                            # keepalive ping time out after 5 seconds, default is 20 seoncds
                                                            ('grpc.keepalive_permit_without_calls', True),
                                                            # allow keepalive pings when there's no gRPC calls
                                                            ('grpc.http2.max_pings_without_data', 0),
                                                            # allow unlimited amount of keepalive pings without data
                                                            ('grpc.http2.min_time_between_pings_ms', 10000),
                                                            # allow grpc pings from client every 10 seconds
                                                            ('grpc.http2.min_ping_interval_without_data_ms', 5000),
                                                            # allow grpc pings from client without data every 5 seconds
                                                        ))
        Outlet_pb2_grpc.add_OutletServicer_to_server(self._grpc_service, server)

        if self.use_zeroconf:
            port = 0
        else:
            port = ensure_int(self.get_config('agent.grpc.fixed_port'))
            logger.debug(f'Config specifies fixed port = {port}')
        port = server.add_insecure_port(f'[::]:{port}')

        if self.use_zeroconf:
            address_list = self.get_local_address_list()

            if not address_list:
                raise RuntimeError('Could not determine local IP address!')
            elif len(address_list) > 1:
                raise RuntimeError(f'Found multiple local IP addresses and dunno which to use: {address_list}')

            self.local_ip = address_list[0]
            fqdn = socket.gethostname()
            hostname = fqdn.split('.')[0]

            desc = {'service': ZEROCONF_SERVICE_NAME, 'version': ZEROCONF_SERVICE_VERSION}
            self.zc_info = ServiceInfo(ZEROCONF_SERVICE_TYPE,
                                       hostname + f' Service.{ZEROCONF_SERVICE_TYPE}',
                                       addresses=[socket.inet_aton(self.local_ip)], port=port, properties=desc)
            self.zeroconf = Zeroconf()
            self.zeroconf.register_service(self.zc_info)
            logger.debug(f'Discoverable service registered via Zeroconf: {self.zc_info}')

        try:
            logger.info(f'gRPC server starting on port {port}...')
            server.start()
            logger.info('gRPC server started!')
            server.wait_for_termination()  # <- blocks
            logger.info('gRPC server stopped!')
        except Exception:
            self.unregister_zeroconf()
            raise

    def unregister_zeroconf(self):
        try:
            if self.zeroconf:
                logger.debug('Unregistering Zeroconf service')
                self.zeroconf.unregister_service(self.zc_info)
                self.zeroconf.close()
            self.zeroconf = None
        except AttributeError:
            pass

    @staticmethod
    def get_local_address_list() -> List[str]:
        address_list: List[str] = []

        interfaces = netifaces.interfaces()
        for i in interfaces:
            if i == 'lo':
                continue
            iface = netifaces.ifaddresses(i).get(netifaces.AF_INET)
            if iface:
                for j in iface:
                    if j['addr'] != LOOPBACK_ADDRESS:
                        address_list.append(j['addr'])
                        logger.info(f'Found local address: {j["addr"]}')

        return address_list
