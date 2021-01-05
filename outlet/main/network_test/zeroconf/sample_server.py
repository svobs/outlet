import socket
import datetime
from zeroconf import ServiceInfo, Zeroconf

HOST = '127.0.0.1'
PORT = 1024  # Port to listen on (non-privileged ports are > 1023)


def main():
    address = ""

    print("#############################")
    print("#### DISCOVERABLE SERVER ####")
    print("#############################\n")

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    host_name = socket.gethostname()
    server_socket.bind((HOST, PORT))
    print(" > Server started on " + socket.gethostname() + ":" + HOST + ":" + str(PORT))

    zeroconf = Zeroconf()

    fqdn = socket.gethostname()
    hostname = fqdn.split('.')[0]

    desc = {'service': 'Discoverable Service', 'version': '1.0.0'}
    info = ServiceInfo('_discoverable._udp.local.',
                       hostname + ' Service._discoverable._udp.local.',
                       addresses=[socket.inet_aton(HOST)], port=PORT, properties=desc)
    try:
        zeroconf.register_service(info)
        print(" > Discoverable service " + str(desc) + " registered:\n" + str(info))

        while True:
            message, address = server_socket.recvfrom(PORT)
            string = message.decode('utf-8')
            print(" > Received: " + message.decode('utf-8') + " from " + str(address))
            server_socket.sendto("dscv_ack".encode('utf-8'), address)
            if "dscv_discover" in string:
                return_message = "dscv_shake:" + HOST
                print(" > Discover call from client: " + str(address))
                print(" > Sending handshake: " + return_message + ", to address: " + str(address))
                server_socket.sendto(return_message.encode('utf-8'), address)
                print(" > [" + str(datetime.datetime.now()) + "] New client connected <" + address[0] + ">")
            elif "dscv_disconnect" in string:
                break
    finally:
        print("\n > Shutting down server [" + str(datetime.datetime.now()) + "]")
        if address != "":
            server_socket.sendto("dscv_disconnect".encode('utf-8'), address)
        zeroconf.unregister_service(info)
        zeroconf.close()
        server_socket.close()


if __name__ == '__main__':
    main()
