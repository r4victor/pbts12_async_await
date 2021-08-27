# echo_06_yield_io.py

import socket

from event_loop_02_io import EventLoopIo


loop = EventLoopIo()


def run_server(host='127.0.0.1', port=55555):
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    sock.listen()
    while True:
        yield 'wait_read', sock
        client_sock, addr = sock.accept()
        print('Connection from', addr)
        loop.create_task(handle_client(client_sock))


def handle_client(sock):
    while True:
        yield 'wait_read', sock
        recieved_data = sock.recv(4096)
        if not recieved_data:
            break
        yield 'wait_write', sock
        sock.sendall(recieved_data)

    print('Client disconnected:', sock.getpeername())
    sock.close()


if __name__ == '__main__':
    loop.create_task(run_server())
    loop.run()