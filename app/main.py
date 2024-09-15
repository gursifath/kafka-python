import socket  # noqa: F401
import struct
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

        api_key_version = 18
        min_version = 0
        max_version = 4
        error_code = 0 if valid_version else 35
        correlation_id = int.from_bytes(data[8:12], byteorder="big")

        response_body = (
            struct.pack(">H", error_code) + 
            struct.pack(">B", 2) + 
            struct.pack(">H", api_key_version) +
            struct.pack(">H", min_version) +
            struct.pack(">H", max_version) + 
            struct.pack(">H", 0) +
            struct.pack(">I", 0)
        )

        response_header = (
            struct.pack(">I", correlation_id)
        )

        message_length = len(response_header) + len(response_body)
        message_length = self.convertToBytes(message_length)
                
        response = message_length + response_header + response_body
        print(response)
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
