import socket  # noqa: F401

class Broker:
    def __init__(self, port):
        self.server = socket.create_server(("localhost", port), reuse_port=True)

    def construct_response(self):
        header = (7).to_bytes(length=4, byteorder="big", signed=True)
        message_length = (len(header)).to_bytes(length=4, byteorder="big", signed=True)
        response = message_length + header
        return response
    
    def handle_request(self, client):
        client.recv(1024)
        client.sendall(self.construct_response())
        client.close()

    def stream(self):
        while True:
            client, addr = self.server.accept()
            self.handle_request(client)

def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    # print("Logs from your program will appear here!")

    broker = Broker(9092)
    broker.stream()

if __name__ == "__main__":
    main()
    