import socket  # noqa: F401

# data = datab'\x00\x00\x00#\x00\x12\x00\x046\xbd\x82c\x00\tkafka-cli\x00\nkafka-cli\x040.1\x00'

class Broker:
    def __init__(self, port):
        self.server = socket.create_server(("localhost", port), reuse_port=True)
        self.client = None
        self.addr = None

    def construct_response(self, data):
        api_version = int.from_bytes(data[6:8], byteorder="big")
        if 0 <= api_version <= 4:
            valid_version = True
        else:
            valid_version = False

        error_code = self.convertToBytes(35, 2)
        correlation_id = self.convertFromBytes(data[8:12])
        header = self.convertToBytes(correlation_id)
        message_length = self.convertToBytes(len(header))
        response = message_length + header + error_code
        return response
    
    def convertToBytes(self, data, length=4):
        return data.to_bytes(length=length, byteorder="big", signed=True)
    
    def convertFromBytes(self, data):
        return int.from_bytes(data, byteorder="big")
    
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
