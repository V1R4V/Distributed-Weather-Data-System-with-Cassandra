import grpc
import station_pb2
import station_pb2_grpc

# As both the client and server are running on container p6-db-1, we can use localhost as the server address
SERVER = "localhost:5440"

def run():
    # Connect to the gRPC server
    with grpc.insecure_channel(SERVER) as channel:
        stub = station_pb2_grpc.StationStub(channel)
        # Send the request and get the table schema
        response = stub.StationSchema(station_pb2.EmptyRequest())
        if response.error:
            print(response.error)
        else:
            # Print the Table(stations)'s schema
            print(response.schema)

if __name__ == '__main__':
    run()