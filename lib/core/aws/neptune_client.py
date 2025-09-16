from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
import os

def get_neptune_connection():
    """
    Returns a Gremlin traversal source and the connection object for Neptune.
    """
    endpoint = os.getenv("NEPTUNE_ENDPOINT")
    port = os.getenv("NEPTUNE_PORT", 8182)
    url = f"wss://{endpoint}:{port}/gremlin"

    graph = Graph()
    connection = graph.traversal().withRemote(DriverRemoteConnection(url, 'g'))
    return connection, connection  # traversal source, connection object
