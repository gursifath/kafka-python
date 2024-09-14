import socket  # noqa: F401

# data = datab'\x00\x00\x00#\x00\x12\x00\x046\xbd\x82c\x00\tkafka-cli\x00\nkafka-cli\x040.1\x00'

class Broker:
    def __init__(self, port):
        self.server = socket.create_server(("localhost", port), reuse_port=True)
        self.client = None
        self.addr = None

    def construct_response(self, data):
        correlation_id = int.from_bytes(data[8:12], byteorder="big")
        header = correlation_id.to_bytes(length=4, byteorder="big", signed=True)
        message_length = (len(header)).to_bytes(length=4, byteorder="big", signed=True)
        response = message_length + header
        return response
    
    def handle_request(self):
        data = self.client.recv(1024)
        self.client.sendall(self.construct_response(data))
        self.client.close()

    def stream(self):
        while True:
            self.client, self.addr = self.server.accept()
            self.handle_request()

def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    # print("Logs from your program will appear here!")

    broker = Broker(9092)
    broker.stream()

if __name__ == "__main__":
    main()
