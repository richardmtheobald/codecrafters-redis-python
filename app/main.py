import asyncio
import os

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

        await writer.drain()

    writer.close()
    await writer.wait_closed()

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
