import socket
import os


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=(os.name != "nt")) #os.name instead of True because Windows doesn't allow it
    connection, address = server_socket.accept()

    while True:
        data = connection.recv(1024)
        if not data:
            break
        connection.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()
