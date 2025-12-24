import grpc
from concurrent import futures
import station_pb2
import station_pb2_grpc
from cassandra.cluster import Cluster
from cassandra.query import ConsistencyLevel
import cassandra
from pyspark.sql import SparkSession
import os

class StationService(station_pb2_grpc.StationServicer):
    def __init__(self):
        prefix = os.environ.get('PROJECT', 'p6')
        cluster = Cluster([f"{prefix}-db-1", f"{prefix}-db-2", f"{prefix}-db-3"])
        self.session = cluster.connect()
        
        self.session.execute("DROP KEYSPACE IF EXISTS weather")
        self.session.execute("""
            CREATE KEYSPACE weather 
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
        """)
        
        self.session.execute("USE weather")
        
        self.session.execute("""
            CREATE TYPE station_record (
                tmin int,
                tmax int
            )
        """)
        
        self.session.execute("""
            CREATE TABLE stations (
                id text,
                date date,
                name text STATIC,
                record station_record,
                PRIMARY KEY (id, date)
            ) WITH CLUSTERING ORDER BY (date ASC)
        """)

        self.spark = SparkSession.builder.appName("p6").getOrCreate()

        df = self.spark.read.text("ghcnd-stations.txt")
        df.createOrReplaceTempView("stations_raw")
        stations = self.spark.sql("""
            SELECT
                SUBSTRING(value, 1, 11) AS id,
                SUBSTRING(value, 39, 2) AS state,
                TRIM(SUBSTRING(value, 42, 30)) AS name
            FROM stations_raw
        """)

        wi_stations = stations.filter("state = 'WI'")
        for row in wi_stations.collect():
            self.session.execute(
                "INSERT INTO weather.stations (id, name) VALUES (%s, %s)",
                (row.id, row.name)
            )

        self.insert_temp = self.session.prepare("""
            INSERT INTO weather.stations (id, date, record) 
            VALUES (?, ?, {tmin: ?, tmax: ?})
        """)
        self.insert_temp.consistency_level = ConsistencyLevel.ONE

        self.select_max_strong = self.session.prepare("""
            SELECT MAX(record.tmax) as max_temp 
            FROM weather.stations 
            WHERE id = ?
        """)
        self.select_max_strong.consistency_level = ConsistencyLevel.THREE

        self.select_max_available = self.session.prepare("""
            SELECT MAX(record.tmax) as max_temp 
            FROM weather.stations 
            WHERE id = ?
        """)
        self.select_max_available.consistency_level = ConsistencyLevel.ONE

        print("Server started")

    def StationSchema(self, request, context):
        result = self.session.execute("DESCRIBE TABLE weather.stations")
        row = result.one()
        schema = row.create_statement
        return station_pb2.StationSchemaReply(schema=schema, error="")

    def StationName(self, request, context):
        result = self.session.execute(
            "SELECT name FROM weather.stations WHERE id = %s",
            (request.station,)
        )
        row = result.one()
        if row:
            return station_pb2.StationNameReply(name=row.name, error="")
        else:
            return station_pb2.StationNameReply(name="", error="Station not found")

    def RecordTemps(self, request, context):
        try:
            self.session.execute(
                self.insert_temp,
                (request.station, request.date, request.tmin, request.tmax)
            )
            return station_pb2.RecordTempsReply(error="")
        except (cassandra.Unavailable, cassandra.cluster.NoHostAvailable):
            return station_pb2.RecordTempsReply(error="unavailable")
        except Exception as e:
            return station_pb2.RecordTempsReply(error=str(e))

    def StationMax(self, request, context):
        try:
            result = self.session.execute(self.select_max_strong, (request.station,))
            row = result.one()
            if row and row.max_temp is not None:
                return station_pb2.StationMaxReply(tmax=row.max_temp, error="")
            else:
                return station_pb2.StationMaxReply(tmax=-1, error="No data found")
        except (cassandra.Unavailable, cassandra.cluster.NoHostAvailable):
            try:
                result = self.session.execute(self.select_max_available, (request.station,))
                row = result.one()
                if row and row.max_temp is not None:
                    return station_pb2.StationMaxReply(tmax=row.max_temp, error="fallback_to_available")
                else:
                    return station_pb2.StationMaxReply(tmax=-1, error="No data found")
            except (cassandra.Unavailable, cassandra.cluster.NoHostAvailable):
                return station_pb2.StationMaxReply(tmax=-1, error="unavailable")
            except Exception as e:
                return station_pb2.StationMaxReply(tmax=-1, error=str(e))
        except Exception as e:
            return station_pb2.StationMaxReply(tmax=-1, error=str(e))


def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=9),
        options=[("grpc.so_reuseport", 0)],
    )
    station_pb2_grpc.add_StationServicer_to_server(StationService(), server)
    server.add_insecure_port('0.0.0.0:5440')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
