import asyncio
import datetime


HOST = '127.0.0.1'
PORT = 55555

BUFSIZE = 4096


def print_indent(indent, string):
    t = datetime.datetime.fromtimestamp(asyncio.get_event_loop().time())
    print('\t' * indent + f'[{t:%S.%f}] ' + string)


async def client(name, indent):
    print_indent(indent, f'Client {name} tries to connect.')
    reader, writer = await asyncio.open_connection(host=HOST, port=PORT)
    # first make dummy write and read to show that the server talks to us
    writer.write(b'*')
    await writer.drain()
    resp = await reader.read(BUFSIZE)
    print_indent(indent, f'Client {name} connects.')

    for msg in ['Hello', 'world!',]:
        await asyncio.sleep(0.5)
        writer.write(msg.encode())
        await writer.drain()
        print_indent(indent, f'Client {name} sends "{msg}".')
        resp = (await reader.read(BUFSIZE)).decode()
        print_indent(indent, f'Client {name} recieves "{resp}".')
    
    writer.close()
    print_indent(indent, f'Client {name} disconnects.')


async def main():
    clients = [asyncio.create_task(client(i, i)) for i in range(3)]
    await asyncio.wait(clients)


if __name__ == '__main__':
    asyncio.run(main())