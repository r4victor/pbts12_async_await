# event_loop_05_thread.py

# This version of the event loop shows how to use sockets and
# I/O multiplexing to implement the to_thread() coroutine
# that runs a specified callable in a separate thread,
# yields the control and resumes when the result is ready.
#
# The event loop is NOT presented in the post.


from collections import deque
import pickle
import selectors
import socket
import time
import types
import threading


class EventLoopThread:
    def __init__(self):
        self.tasks_to_run = deque([])
        self.sel = selectors.DefaultSelector()

    def create_task(self, coro):
        self.tasks_to_run.append(coro)

    @types.coroutine
    def sock_recv(self, sock, n):
        yield 'wait_read', sock
        return sock.recv(n)

    @types.coroutine
    def sock_sendall(self, sock, data):
        yield 'wait_write', sock
        sock.sendall(data)
    
    @types.coroutine
    def sock_accept(self, sock):
        yield 'wait_read', sock
        return sock.accept()

    @types.coroutine
    def to_thread(self, callable):
        def callable_wrapper():
            result = callable()
            sock1.sendall(pickle.dumps(result))

        sock1, sock2 = socket.socketpair()
        threading.Thread(target=callable_wrapper).start()
        yield 'wait_read', sock2
        return pickle.loads(sock2.recv(4096))

    def run(self):
        while True:
            if self.tasks_to_run:
                task = self.tasks_to_run.popleft()
                try:
                    op, arg = task.send(None)
                except StopIteration:
                    continue

                if op == 'wait_read':
                    self.sel.register(arg, selectors.EVENT_READ, task)
                elif op == 'wait_write':
                    self.sel.register(arg, selectors.EVENT_WRITE, task)
                else:
                    raise ValueError('Unknown event loop operation:', op)
            else:
                for key, _ in self.sel.select():
                    task = key.data
                    sock = key.fileobj
                    self.sel.unregister(sock)
                    self.create_task(task)


# How to use to_thread()


def compute():
    # some long computation
    time.sleep(2)
    return 2


async def coro():
    res =  await loop.to_thread(compute)
    print(res)


if __name__ == '__main__':
    loop = EventLoopThread()
    loop.create_task(coro())
    loop.run()
    # Ctrl+C to stop