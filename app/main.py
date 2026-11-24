import asyncio
import os
import time
import random

store = {}

async def handle_client(reader, writer):
    while True:
        data = await reader.read(1024)
        if not data:
            break


        command, _ = parse_resp(data)
        name = command[0].upper()

        match [name, *command[1:]]:
            case [b"PING"]:
                writer.write(b"+PONG\r\n")

            case [b"ECHO", message]:
                writer.write(encode_bulk_string(message))

            case [b"SET", key, value, *options]:
                expires_at = None
                i = 0
                while i < len(options):
                    option = options[i].upper()
                    if option == b"PX":
                        expires_at = time.monotonic() + int(options[i+1]) / 1000
                        i+=2
                    elif option == b"EX":
                        expires_at = time.monotonic() + int(options[i+1])
                        i+=2
                    else:
                        i+=1
                store[key] = (value, expires_at)
                writer.write(b"+OK\r\n")

            case [b"GET", key]:
                value=get_value(key)
                if value is not None:
                    writer.write(encode_bulk_string(value))
                else:
                    writer.write(b"$-1\r\n")

            case [b"RPUSH", key, *elements]:
                current = get_value(key)
                if current is None:
                    current = []
                    store[key] = (current, None)
                current.extend(elements)
                writer.write(encode_integer(len(current)))

        await writer.drain()

    writer.close()
    await writer.wait_closed()

def encode_integer(number):
    return b":" + str(number).encode() + b"\r\n"

def remove_value_if_expired(key):
    entry = store.get(key)
    if entry is None:
        return
    value, expires_at = entry
    if expires_at is not None and time.monotonic() >= expires_at:
        del store[key]

def get_value(key):
    num = random.randint(1, 50)
    if num == 7:# simple garbage collection for expired items in store
        for item in list(store):
            remove_value_if_expired(item)

    remove_value_if_expired(key)
    entry = store.get(key)
    if entry is None:
        return None
    return entry[0]

def parse_resp(data, position=0):
    type_char = data[position:position+1]
    line_end = data.index(b"\r\n",position)
    header = data[position + 1:line_end]
    position = line_end + 2

    if type_char == b"*":
        items = []
        for _ in range(int(header)):
            item, position = parse_resp(data, position)
            items.append(item)
        return items, position

    if type_char == b"$":
        length = int(header)
        return data[position:position + length], position + length + 2

    if type_char == b"+":
        return header, position

    if type_char == b":":
        return int(header), position

def encode_bulk_string(value):
    return b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"

async def main():
    server = await asyncio.start_server(handle_client, "localhost", 6379)
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
