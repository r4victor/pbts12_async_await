# echo_07_yield_from.py

import socket

from event_loop_03_yield_from import EventLoopYieldFrom


loop = EventLoopYieldFrom()


def run_sever(host='127.0.0.1', port=55555):
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen()
    while True:
        client_sock, addr = yield from loop.sock_accept(sock)
        print('Connection from', addr)
        loop.create_task(handle_client(client_sock))


def handle_client(sock):
    while True:
        recieved_data = yield from loop.sock_recv(sock, 4096)
        if not recieved_data:
            break
        yield from loop.sock_sendall(sock, recieved_data)

    print('Client disconnected:', sock.getpeername())
    sock.close()


if __name__ == '__main__':
    loop.create_task(run_sever())
    loop.run()