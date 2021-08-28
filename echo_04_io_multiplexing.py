# echo_04_io_multiplexing.py

import socket
import selectors


sel = selectors.DefaultSelector()


def setup_listening_socket(host='127.0.0.1', port=55555):
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen()
    sel.register(sock, selectors.EVENT_READ, accept)


def accept(sock):
    client_sock, addr = sock.accept()
    print('Connection from', addr)
    sel.register(client_sock, selectors.EVENT_READ, recv_and_send)


def recv_and_send(sock):
    received_data = sock.recv(4096)
    if received_data:
        # assume sendall won't block
        sock.sendall(received_data)
    else:
        print('Client disconnected:', sock.getpeername())
        sel.unregister(sock)
        sock.close()


def run_event_loop():
    while True:
        for key, _ in sel.select():
            callback = key.data
            sock = key.fileobj
            callback(sock)


if __name__ == '__main__':
    setup_listening_socket()
    run_event_loop()